#!/usr/bin/env python3

from __future__ import print_function

import copy
import random
import time
import redis, pickle
from bisect import bisect
from collections import defaultdict

import numpy as np

from .schedule import Scheduler
from .telemetry import Telemetry
from .trace import Tracer
from .utils import gb, timed

"""
Orchestrate the simulation and manage the cluster states

Assumptions:
    - independent from the scheduling on other resource constraints, e.g., cpu/memory/net
"""


class Simulator:
    def __init__(self):
        self.tr = Tracer()

    def init_cluster(self,
                     num_node=100,
                     precached=False,
                     cached_rank=10,
                     store_size=4 * gb,
                     cont_cap=100,
                     pinned=False,
                     ):

        # start with each node empty
        # [[dict{str_image_name:[int_ctr, int_last_used, int_freq_used, bool_pinned]...},
        #   dict{str_layer_digest:[int_ctr, int_last_used, int_freq_used, pinned]...},
        #   int_cur_size, int_max_size, int_conta, int_max_conta,
        #   dict{int_ddl: [str_image_name,...]}, real_free_size]]
        self.node_list = [[defaultdict(lambda: [0, 0, 0, False]),
                           defaultdict(lambda: [0, 0, 0, False]),
                           0, store_size, 0, cont_cap,
                           defaultdict(list), store_size, list] for _ in range(num_node)]

        if precached:
            print("--> precached mode: warming up the cache..")
            if cached_rank < 0:
                # randomly warm up the cache
                for node in self.node_list:
                    sampled_images = self.tr.get_random_images(count=200)
                    for image in sampled_images:
                        image_size = self.tr.image_size(image)
                        if node[2] + image_size > node[3] * (1 - self.evict_th):
                            break
                        self.place_node(0, (image, 0), node, pinned=pinned, image_only=True)
            else:
                images = self.tr.get_top_images(cached_rank)
                for node in self.node_list:
                    for image in images:
                        self.place_node(0, (image, 0), node, pinned=pinned, image_only=True)

        print("--> new cluster init.")

    @timed
    def init_req_queue(self, sim_length=100, req_rate=40,
                       uniform=False, cont_length=1000, zipf=False,
                       max_num_image=1, from_cache=False):
        # [(str_image_name, int_duration)...]

        self.req_list = []
        self.req_seq = []

        r = redis.StrictRedis(host='localhost')
        num_req = sim_length * req_rate
        self.duration = num_req // req_rate
        self.tr.set_metric("duration", self.duration)
        self.cont_length = cont_length
        self.sim_length = sim_length
        self.req_rate = req_rate

        while sum(self.req_seq) < num_req:
            self.req_seq.append(np.random.poisson(req_rate))
        diff = sum(self.req_seq) - num_req
        num_req += diff

        # sample a set of images uniformly as requests
        if uniform and not zipf and not from_cache:
            image_list = self.tr.image_list_()
            while len(self.req_list) < num_req:
                num_image_reg = random.randint(1, max_num_image)
                self.req_list.append([random.sample(image_list, num_image_reg),
                                      int(random.random() * cont_length + 1)])
            r.set("req_list", pickle.dumps(self.req_list))
        # sample a set of images with weights/popularity as the requests
        elif not from_cache:
            image_pop_list = self.tr.image_pop_list_() \
                if not zipf else self.tr.image_pop_list_zipf_()
            self._gen_cumweights(image_pop_list)
            while len(self.req_list) < num_req:
                num_image_reg = random.randint(1, max_num_image)
                images = self.weighted_sample(image_pop_list, num_image_reg)
                self.req_list.append(
                    [images, int(random.random() * cont_length + 1)])
            r.set("req_list", pickle.dumps(self.req_list))
        else:
            start = time.time()
            self.req_list = pickle.loads(r.get("req_list"))
            print("loading {} ".format(time.time() - start))
        # self.tr.analyze_req_list(self.req_list)

        self.seed_pool = random.sample(list(range(num_req)) * 4, num_req)

        # print("--> debug: requests are: ", self.req_list)
        print("--> request queue init.")

    def is_all_full(self, nodes):
        for n in nodes:
            if n[3] - n[2] > 0:
                return False
        return True

    # @timed
    def calc_latency(self, req, node, num_puller):
        # report the pull latency of the image-node pair, in milliseconds
        node_images, node_layers = node[0], node[1]
        req_images = req[0]
        time = 0

        # short cut if all images are present on the nodes
        all_in = True
        for image in req_images:
            if image not in node_images:
                all_in = False
                break
        if all_in:
            return time
            # print("debug: missing image: ", req)

        # the true pulling time is given at the layer level
        for image in req_images:
            req_layers = self.tr.layers_(image)
            for l in req_layers:
                if l not in node_layers:
                    time += self.tr.layer_dl_time(l)
                    time += self.tr.layer_reg_time(l)
        return time

    # @timed
    def place_node(self, sig, req, node, *, pinned=False, image_only=False):
        node_images, node_layers, containers = node[0], node[1], node[6]
        req_images, duration = req[0], req[1]

        req_layers = set()
        for image in req_images:
            req_layers.update(self.tr.layers_(image))

        provision_lat, required_size = 0, 0
        for digest in req_layers:
            new_layer = False
            if digest not in node_layers:
                size = self.tr.layer_size(digest)
                node[2] += size
                node[7] -= size
                required_size += size
                provision_lat += self.tr.layer_pull_time(digest)
                new_layer = True
            layer = node_layers[digest]
            layer[0] += 1
            layer[1] = sig
            layer[2] += 1
            layer[3] = pinned
            if not new_layer and layer[0] == 1 and not layer[3]:
                node[7] -= self.tr.layer_size(digest)

        # convert to seconds
        ddl = round(sig + provision_lat / 1000 + duration)

        for image in req_images:
            node_images[image][0] += 1
            node_images[image][1] = sig
            node_images[image][2] += 1
            node_images[image][3] = pinned
            if not image_only:
                containers[ddl].append(image)
                node[4] += 1  # update the live container counts
        assert node[7] >= 0
        return required_size

    # @timed
    def evict_node(self, node, *,
                   evict_policy,
                   evict_th=0.1,
                   ):
        images, layers, containers = node[0], node[1], node[6]
        used, cap, freed = node[2], node[3], 0
        available = cap - used

        # gc_target defines the target free space the node should have
        gc_target = (1 - evict_th) * node[3]
        # layer based eviction
        if self.evict_dep and evict_policy in {"dep-lru", "dep-lfu"}:
            if evict_policy == "dep-lru":
                sort_index = 1
            elif evict_policy == "dep-lfu":
                sort_index = 2
            else:
                raise Exception("--> unknown layer-based eviction: {}.".format(evict_policy))

            layer_gc_list = sorted(layers.items(), key=lambda x: x[1][sort_index])

            for digest, state in layer_gc_list:
                if node[2] < gc_target:
                    break

                if state[0] > 0 or state[3]:
                    continue

                layers.pop(digest)
                layer_size = self.tr.layer_size(digest)
                freed += layer_size
                node[2] -= layer_size

                # make sure images having layers evicted are also evicted
                layer_images = self.tr.layer_image(digest)
                for image in layer_images:
                    if image in images:
                        images.pop(image)
                self.ty.report_gc(layer_size)
        else:
            # image-based eviction; default: LRU-image; sorted from
            # the most recent to the most early to use the list as a
            # stack more efficiently
            image_gc_list = sorted(images.items(), key=lambda x: x[1][1], reverse=True)
            for name, state in image_gc_list:
                if node[2] < gc_target:
                    break
                # handling image removal
                # state: [int_ctr, int_last_used, int_freq_used, bool_pinned].
                if state[0] > 0 or state[3]:
                    continue
                images.pop(name)
                # handling layer removal
                image_layers = self.tr.layers_(name)
                for digest in image_layers:
                    if digest not in layers or layers[digest][0] > 0:
                        continue
                    shared = False
                    layer_images = self.tr.layer_image(digest)
                    # make sure images having layers evicted will also be
                    # removed from record; if the layer is shared by other
                    # images, do not remove the layer until the last image
                    # using it is removed. This would guarantee correctness.
                    # TODO: add a share counter for the layer state
                    for name in layer_images:
                        if name in images:
                            shared = True
                            break
                    if not shared:
                        assert digest in layers
                        layers.pop(digest)
                        layer_size = self.tr.layer_size(digest)
                        freed += layer_size
                        node[2] -= layer_size
                        self.ty.report_gc(layer_size)
        available += freed
        assert node[2] >= 0, "--> used space should be greater than 0, {} given.".format(node[2])
        assert node[7] >= 0
        assert node[2] <= node[3], "--> used exceeds capacity: {}/{}".format(node[2], node[3])
        assert 0 <= node[4] <= node[5], \
            "--> erroneous container count: {}/{}".format(node[4], node[5])
        # the assertion below can be time consuming
        # assert sum([self.tr.layer_size(l) for l in layers]) == node[2], \
        #     "--> total layer size mismatch. {}/{}".format(sum([self.tr.layer_size(l) for l in layers]), node[2])
        # assert available >= gc_target or node[5] == node[4], \
        #     "--> unable to free enough space: {}/{}/{}/{}.".format(available, freed, gc_target, node[4])
        return available

    def real_free_space(self, node):
        used = sum([self.tr.layer_size(digest) for digest, layer in node[1].items() if layer[0] > 0])
        return node[3] - used

    # @timed
    def update_nodes(self, sig, nodes, *,
                     evict_interval=1,
                     evict_policy="kube",
                     evict_th=0.1):
        for node in nodes:
            images, layers, containers = node[0], node[1], node[6]

            # remove containers, update counters and obtain a list of image
            # to garbage collect
            if sig in containers:
                for image_name in containers[sig]:
                    # assertion to avoid default dict generate any images
                    assert image_name in images
                    node[4] -= 1  # container counts

                    if images[image_name][0] > 0:
                        images[image_name][0] -= 1

                    for digest in self.tr.layers_(image_name):
                        assert digest in layers
                        layer = layers[digest]
                        if layer[0] > 0:
                            layer[0] -= 1
                            if layer[0] == 0 and not layer[3]:
                                node[7] += self.tr.layer_size(digest)
            assert node[7] <= node[3], str((node[7], node[3]))
            # skip if eviction not enabled or not at the right moment
            if self.evict and (sig % evict_interval) == 0:
                self.evict_node(node,
                                evict_policy=evict_policy,
                                evict_th=evict_th)

    def node_heating_ratio(self):
        node_load = [n[4] for n in self.node_list]
        avg_load = np.average(node_load)
        if avg_load == 0:
            avg_load = 1
        max_load = np.max(node_load)
        return max_load/avg_load

    def _sim(self, *, max_sim_duration=10 ** 10,
             delay_sched=False, delay=0, provision_gap=5,
             policies=("dep", "kube", "monkey"),
             evict_policy="dep-lru",
             evict_th=0.1,
             lb_ratio=None,
             hot_duration=0,
             ):
        # save a deep copy of the node list
        node_list = copy.deepcopy(self.node_list)

        # store the results of interests such that the experiments can
        # conveniently obtain the them after a single run
        quick_results = defaultdict(list)

        for policy in policies:
            # reset metric and counters
            self.ty.reset()
            total_lat, total_provision_lat, accept_req_num, req_seq_pos = 0, 0, 0, 0
            evict_policy = evict_policy if policy == "dep" else "kube"
            retry_queue = []
            node_heatings = []

            # simulation loop
            for tick in range(max_sim_duration):
                # print(tick)
                # update cluster states, using t as the event signal
                sig = tick
                # print(self.node_heating_ratio())
                # if 0.5 * len(self.req_seq) < tick < len(self.req_seq):
                #     self.ty.tel_node_snap(self.node_list, image=True)

                if 0.0 * len(self.req_seq) < tick < len(self.req_seq):
                    node_heatings.append(self.node_heating_ratio())
                    self.ty.tel_node_snap(self.node_list, image=False)

                self.update_nodes(sig, self.node_list, evict_policy=evict_policy, evict_th=self.evict_th)

                sig, req_node_pairs = tick, []
                if tick >= len(self.req_seq):
                    req_batch = []
                    if len(retry_queue) == 0:
                        self.tr.set_metric("duration", tick)
                        break
                else:
                    # concatenate the retry_queue and the req_batch is equivalent of leaving
                    # unscheduled tasks in one single queue with new ones appended at the end
                    req_batch = [(sig, req) for req in self.req_list[req_seq_pos:req_seq_pos + self.req_seq[tick]]]
                    req_seq_pos += self.req_seq[tick]

                # print(len(retry_queue))
                req_batch = retry_queue + req_batch
                retry_queue = []

                # scheduling loop
                for submit_tick, req in req_batch:
                    # fast check if all nodes are full at the moment
                    if self.is_all_full(self.node_list):
                        retry_queue.append((submit_tick if submit_tick < sig else sig, req))
                        continue

                    # scheduler finds the node to place the request, -1 if failed
                    node_index = self.sched.schedule(req, self.node_list,
                                                     policy, lb_ratio=lb_ratio)
                    if node_index < 0:
                        self.ty.report_rej(node_index)
                        retry_queue.append((submit_tick if submit_tick < sig else sig, req))
                        continue

                    node = self.node_list[node_index]
                    provision_lat = self.calc_latency(req, node)
                    wait_time = (sig - submit_tick) * 1000

                    # delay scheduling
                    if delay_sched and policy is "dep":
                        # if the "best" node found still yields too high startup latency, wait a bit
                        if provision_lat > provision_gap * (1 + wait_time) and wait_time <= delay * 1000:
                            retry_queue.append((submit_tick if submit_tick < sig else sig, req))
                            continue
                    # node placement
                    required_size = self.place_node(sig, req, node)
                    if node[2] + required_size > node[3] * (1 - evict_th):
                        free_size = self.evict_node(node,
                                                    evict_policy=evict_policy,
                                                    )
                    # collect metrics
                    lat = provision_lat + wait_time
                    total_lat += lat
                    total_provision_lat += provision_lat
                    accept_req_num += 1
                    self.tr.add_lat_result(lat)
                    self.tr.add_provision_lat_result(provision_lat)
                    self.ty.report_req(req)
                    # end of simulation loop

            # collect results
            if tick == max_sim_duration:
                print("--> max sim duration hit.")
                quick_results["util"].append(1)

            mean_startup_lat = -1.0
            if accept_req_num != 0:
                mean_startup_lat = round(total_lat / accept_req_num)

            mean_provision_lat = -1.0
            if accept_req_num != 0:
                mean_provision_lat = round(total_provision_lat / accept_req_num)

            self.tr.set_metric("mean_lat", mean_startup_lat) \
                .set_metric("mean_provision_lat", mean_provision_lat) \
                .set_metric("accept_req_num", accept_req_num) \
                .set_setup_metric("sched_policy", policy)

            quick_results["util"].append(self.ty.tel_util(total_provision_lat))
            quick_results["mean_provision_lat"].append(mean_provision_lat)
            quick_results["mean_startup_lat"].append(mean_startup_lat)

            # print and write out results
            self.ty.tel_blank()
            self.ty.tel_gc()
            self.ty.tel_rej()
            # self.ty.tel_nodes(self.node_list, "last")
            self.tr.cal_lat_percentile()
            self.tr.dump_meta_result(policy)
            self.tr.dump_lat_result(policy)

            # reset the internal node_list
            self.node_list = copy.deepcopy(node_list)
            self.ty.reduce_node_snap()
            print(np.percentile(node_heatings, 99))
        if len(policies) > 1:
            baseline = None
            for i, data in enumerate(quick_results["mean_startup_lat"]):
                if i == 0:
                    baseline = data
                else:
                    quick_results["speedup"].append(data / baseline)
        return quick_results

    def sim(self, result_dir="./result/",
            precached=False, pinned=False, uniform=True,
            node_num=100, sim_length=100, req_rate=40,
            store_size=3.2 * gb, cont_cap=20, evict=False,
            cont_length=1000, dep_th=0.1, evict_th=0.1,
            evict_dep=False, rerun=False,
            cached_rank=10, delay_sched=False,
            delay=0, policies=["dep", "kube", "monkey"],
            max_num_image=1,
            evict_policy="dep-lru",
            provision_gap=1, zipf=False, lb_ratio=None,
            hot_duration=0):
        """
        simulation modes:
            warmup: start with empty nodes
            precache: start with nodes containing caches
        metrices:
            req_num, accept_req_num
            mean_lat(ency), total_lat
            [bw_sav(ing)]
        """
        # parameter checks
        # 1 gb is the assumed largest container image size

        self.tr.set_result_dir(result_dir)
        self.evict = evict
        self.evict_th = evict_th
        self.evict_dep = evict_dep

        if rerun:
            self.update_cluster(
                node_num,
                store_size=store_size,
                cont_cap=cont_cap,
                precached=precached,
                cached_rank=cached_rank,
                pinned=pinned)
            self.update_req_queue(
                sim_length,
                uniform=uniform,
                cont_length=cont_length,
                req_rate=req_rate,
                zipf=zipf,
                max_num_image=max_num_image)
        else:
            self.init_cluster(node_num, store_size=store_size,
                              cont_cap=cont_cap, precached=precached,
                              cached_rank=cached_rank)
            self.init_req_queue(sim_length, uniform=uniform, cont_length=cont_length,
                                req_rate=req_rate, zipf=zipf, max_num_image=max_num_image)

        self.tr.set_setup_metric("sim_length", sim_length) \
            .set_setup_metric("req_rate", req_rate) \
            .set_setup_metric("max_num_image", max_num_image) \
            .set_setup_metric("node_num", node_num) \
            .set_setup_metric("is_uniform", uniform) \
            .set_setup_metric("zipf", zipf) \
            .set_setup_metric("cont_cap", cont_cap) \
            .set_setup_metric("cont_length", cont_length) \
            .set_setup_metric("store_size", str(store_size / gb) + "GB") \
            .set_setup_metric("evict_th", evict_th) \
            .set_setup_metric("evict_dep", evict_dep) \
            .set_setup_metric("evict_policy", evict_policy) \
            .set_setup_metric("is_setup_evict", evict) \
            .set_setup_metric("cached_rank", cached_rank) \
            .set_setup_metric("precached", precached) \
            .set_setup_metric("delay_sched", delay_sched) \
            .set_setup_metric("delay", delay) \
            .set_setup_metric("provision_gap", provision_gap) \
            .set_setup_metric("lb_ratio", lb_ratio) \
            .set_setup_metric("hot_duration", hot_duration) \
            .set_metric("accept_req_num", 0) \
            .set_metric("mean_lat", 0)

        self.sched = Scheduler(self.tr, dep_th)
        self.ty = Telemetry(tracer=self.tr, verbose=0)
        print("--> running simulation..")

        return self._sim(delay_sched=delay_sched,
                         delay=delay,
                         provision_gap=provision_gap,
                         policies=policies,
                         evict_policy=evict_policy,
                         lb_ratio=lb_ratio,
                         hot_duration=hot_duration)

    def update_cluster(self, node_num, *, store_size, cont_cap, precached, cached_rank, pinned):
        if node_num != len(self.node_list) or self.tr.get_metric("cached_rank") != cached_rank \
                or self.tr.get_metric("precached") != precached:
            self.init_cluster(node_num, store_size=store_size,
                              cont_cap=cont_cap, precached=precached, cached_rank=cached_rank, pinned=pinned)
            return
        if self.tr.get_metric("store_size") != str(store_size / gb) + "GB":
            for n in self.node_list:
                n[3] = store_size
            print("--> updated layer store capacity.")
        if self.tr.get_metric("cont_cap") != cont_cap:
            for n in self.node_list:
                n[5] = cont_cap
            print("--> updated container capacity.")

    def update_req_queue(
            self,
            sim_length,
            uniform,
            cont_length,
            req_rate,
            zipf,
            max_num_image):
        if sim_length != self.sim_length or req_rate != self.req_rate or self.tr.get_metric(
                "is_uniform") != uniform or self.tr.get_metric("zipf") != zipf:
            self.init_req_queue(
                sim_length,
                uniform=uniform,
                cont_length=cont_length,
                req_rate=req_rate,
                zipf=zipf,
                max_num_image=max_num_image)
            return
        if self.tr.get_metric("cont_length") != cont_length:
            for i in range(len(self.req_list)):
                self.req_list[i] = (self.req_list[i][0],
                                    int(random.random() * cont_length + 1))
            print("--> updated maximum container running time.")

    def _gen_cumweights(self, source):
        _, weights = zip(*source)
        self.total = 0
        self.cum_weights = []

        for w in weights:
            self.total += w * 10 ** 8
            self.cum_weights.append(self.total)

    def weighted_sample(self, source, num_sample=1):
        results = list()

        for s in random.sample(range(int(self.total)), num_sample):
            i = bisect(self.cum_weights, s)
            results.append(source[i][0])
        return results


def main():
    print("--> please run via cmd.py")


if __name__ == '__main__':
    main()
