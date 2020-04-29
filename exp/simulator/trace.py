#!/usr/bin/env python3

from __future__ import print_function

import cloudpickle as pickle
import os
import random
from collections import defaultdict, namedtuple

import numpy as np
import redis

from .utils import dir_check, ts_gen, time_func, gb, mb, Singleton
from ..share_study.ecr import ECRImageDB

"""
The simulation results are in the following form:
    - image name
    - .csv is appended to the file name when used
each item is a column, separated by comma
"""

SERIAL_NUM = ts_gen()
BASE_DIR = "./__result__/"
LAT_RESULTS = "./__result__/out_lat"
PROVISION_LAT_RESULTS = "./__result__/out_provision_lat"
META_RESULTS = "./__result__/out_meta"
MAX_IMAGE_SIZE = 10.2 * gb
_dir_path = os.path.dirname(os.path.realpath(__file__))
_result_path = _dir_path + "/__plot__/__data__/"


class Tracer(ECRImageDB, metaclass=Singleton):
    def __init__(self):
        self.image_pop_list = None
        self.image_pop_list_zipf = None
        self.layer_pop_list = None

        self.metric_map = defaultdict()
        self.metric_setup_map = defaultdict()
        self.lat_list = []
        self.provision_lat_list = []

        self.cdf = defaultdict(list)
        self.hist = defaultdict(list)

        self.load()
        print("--> tracer init.")

    def load(self):
        try:
            self.load_from_redis()
            # self.load_from_db()
            # self.image_pop_list_zipf_()
        except:
            self.dump_to_redis()
            self.load_from_redis()

    @time_func
    def dump_to_redis(self):
        self.load_from_db()
        r = redis.StrictRedis(host='localhost')
        r.set("imageinfo", pickle.dumps(self.imageinfo_map))
        r.set("layerinfo", pickle.dumps(self.layerinfo_map))
        r.set("layerpull", pickle.dumps(self.layerpull_map))
        r.set("imagelist", pickle.dumps(self.image_list))

    @time_func
    def load_from_redis(self):
        r = redis.StrictRedis(host='localhost')
        self.imageinfo_map = pickle.loads(r.get("imageinfo"))
        self.layerinfo_map = pickle.loads(r.get("layerinfo"))
        self.layerpull_map = pickle.loads(r.get("layerpull"))
        self.image_list = pickle.loads(r.get("imagelist"))

    @time_func
    def load_from_db(self, apply_filter=True, *,
                     pcr_images=True,
                     filter=namedtuple("filter", ["max_size"])(MAX_IMAGE_SIZE),
                     ):

        """Translate database entries to tracer's internal data structures."""
        # {str_image_name : [int_#_pull, int_size_bytes, set{str_layer_digest, ...}]}
        self.imageinfo_map = defaultdict(lambda: [0, 0, set()])
        # {str_layer_digest : [int_#_pull, int_size_bytes, set{str_image, ...}]}
        self.layerinfo_map = defaultdict(lambda: [0, 0, set()])
        # {str_layer_digest: (int_dl_time, int_reg_time)}
        self.layerpull_map = defaultdict(lambda: [0, 0])
        # [str_image_name, ...]
        self.image_list = []

        # TODO: update the API to remove the internal translation
        # As images may be filtered out, we add layers by image, and refer to
        # the database entry for values.
        ecr_layers = {entry["digest"]: entry for entry in self.ecr_layers.find()}
        ecr_images = self.ecr_images.find()

        for entry in ecr_images:
            if apply_filter and self.filter_image(entry, filter):
                continue
            self.image_list.append(entry["name"])
            new_entry = self.imageinfo_map[entry["name"]]
            new_entry[0] += entry["popularity"]
            new_entry[1] += entry["size"]
            new_entry[2] = set(entry["layers"])
            for digest in entry["layers"]:
                if digest in self.layerinfo_map:
                    continue
                layer_entry = ecr_layers[digest]
                new_layer_entry = self.layerinfo_map[digest]
                new_layer_entry[0] = layer_entry["popularity"]
                new_layer_entry[1] = layer_entry["size"]
                new_layer_entry[2] = set(layer_entry["images"])
                self.layerpull_map[digest] = (layer_entry["dl_time"],
                                              layer_entry["reg_time"])

        if not pcr_images:
            return

        ecr_layers = {entry["digest"]: entry for entry in self.pcr_layers.find()}
        real_ecr_layers = {entry["digest"]: entry for entry in self.ecr_layers.find()}
        ecr_images = self.pcr_images.find()

        for entry in ecr_images:
            if apply_filter and self.filter_image(entry, filter):
                continue
            name = entry["name"]
            self.image_list.append(name)
            new_entry = self.imageinfo_map[name]
            new_entry[0] += entry["popularity"]
            new_entry[1] += entry["size"]
            new_entry[2] = set(entry["layers"])
            for digest in entry["layers"]:
                if digest in self.layerinfo_map:
                    self.layerinfo_map[digest][2].add(name)
                    continue
                if digest in ecr_layers:
                    layer_entry = ecr_layers[digest]
                else:
                    layer_entry = real_ecr_layers[digest]
                new_layer_entry = self.layerinfo_map[digest]
                new_layer_entry[0] = layer_entry["popularity"]
                new_layer_entry[1] = layer_entry["size"]
                new_layer_entry[2] = set(layer_entry["images"])
                self.layerpull_map[digest] = (layer_entry["dl_time"],
                                              layer_entry["reg_time"])
        # import pprint as pp
        # pp.pprint(self.layerpull_map)

    def subsample_images(self, reset=False):
        pass

    def set_result_dir(self, base_dir="./__result__/"):
        dir_check(base_dir)
        global BASE_DIR, LAT_RESULTS, META_RESULTS, PROVISION_LAT_RESULTS
        BASE_DIR = base_dir
        LAT_RESULTS = base_dir + "out_lat"
        PROVISION_LAT_RESULTS = base_dir + "out_provision_lat"
        META_RESULTS = base_dir + "out_meta"

    def formatted_metrics(self):
        line = [
            list(self.metric_map.items()) +
            ["|||"] +
            list(self.metric_setup_map.items())]
        return line

    def dump_meta_result(self, policy):
        print(self.formatted_metrics())
        print_metrics = [
            "cont_limit_rej",
            "store_limit_rej",
            "mean_lat",
            "rej_ratio"]
        with open(META_RESULTS + "_" + policy + "_" + SERIAL_NUM + ".csv", "w") as f:
            for m in print_metrics:
                value = str(self.metric_map[m])
                f.write(m + "," + value + "\n")

    def dump_lat_result(self, policy):
        print("--> latencies in percentiles: ", self.percent_lat)

        with open(LAT_RESULTS + "_percentiles" + "_" + policy + "_" + SERIAL_NUM + ".csv", "w") as f:
            f.write(",".join([str(l)
                              for l in self.percent_lat]))

        with open(LAT_RESULTS + "_" + policy + "_" + SERIAL_NUM + ".csv", "w") as f:
            for l in self.lat_list:
                f.write(str(l) + "\n")

        # reset here since we keep the result states in this class object
        self.lat_list = []

    def dump_provision_lat_result(self, policy):
        print("--> provision latencies in percentiles: ", self.percent_provision_lat)

        with open(PROVISION_LAT_RESULTS + "_percentiles" + "_" + policy + "_" + SERIAL_NUM + ".csv", "w") as f:
            f.write(",".join([str(l)
                              for l in self.percent_provision_lat]))

        with open(PROVISION_LAT_RESULTS + "_" + policy + "_" + SERIAL_NUM + ".csv", "w") as f:
            for l in self.lat_list:
                f.write(str(l) + "\n")

        # reset here since we keep the result states in this class object
        self.provision_lat_list = []

    def cal_lat_percentile(self, percentiles=(5, 25, 50, 75, 95)):
        """ also attach the mean latency at the end """
        lat_list = sorted(self.lat_list)
        loc_list = [int(p * 0.01 * len(lat_list)) for p in percentiles]
        self.percent_lat = [lat_list[i] for i in loc_list]
        self.percent_lat.append(self.metric_map["mean_lat"])
        # print(self.percent_lat)

    def cal_provision_lat_percentile(self, percentiles=(5, 25, 50, 75, 95)):
        """ also attach the mean latency at the end """
        lat_list = sorted(self.provision_lat_list)
        loc_list = [int(p * 0.01 * len(lat_list)) for p in percentiles]
        self.percent_provision_lat = [lat_list[i] for i in loc_list]
        self.percent_provision_lat.append(self.metric_map["mean_provision_lat"])
        print(self.percent_provision_lat)

    def set_metric(self, key, value):
        self.metric_map[key] = value
        return self

    def set_setup_metric(self, key, value):
        self.metric_setup_map[key] = value
        return self

    def get_metric(self, key):
        if key in self.metric_map:
            return self.metric_map[key]
        else:
            return self.metric_setup_map[key]

    def add_lat_result(self, lat):
        self.lat_list.append(lat)

    def add_provision_lat_result(self, lat):
        self.provision_lat_list.append(lat)

    def image_list_(self):
        return self.image_list

    def get_top_images(self, rank):
        rank_list = sorted(self.image_pop_list_(), key=lambda x: x[1], reverse=True)[:rank]
        return [i[0] for i in rank_list]

    def get_random_images(self, count=1):
        return random.sample(self.image_list, count)

    def image_size(self, image):
        return self.imageinfo_map[image][1]

    def image_pop_list_(self):
        if self.image_pop_list is None:
            self.image_pop_list = [(k, v[0])
                                   for k, v in self.imageinfo_map.items() if k in self.image_list]
        return self.image_pop_list

    def image_pop_list_zipf_(self, alpha=0.75):
        if self.image_pop_list_zipf is None:
            self.image_pop_list_zipf = [(name, entry[0]) for name, entry in
                                        self.imageinfo_map.items() if name in self.image_list]
            # replace the popularity with zipf samples
            num_images = len(self.image_pop_list_zipf)

            def zipf(rank):
                omega = (1 - alpha) / pow(num_images, 1 - alpha)
                return omega / pow(rank, alpha)

            self.image_pop_list_zipf = [(entry[0], zipf(rank + 1)) for rank, entry in
                                        enumerate(sorted(self.image_pop_list_zipf,
                                                         key=lambda x: x[1], reverse=True))]
        # print(sorted(self.image_pop_list_zipf,key=lambda x:x[1], reverse=True)[:10])
        # r = redis.StrictRedis(host='localhost')
        # r.set("image_pop_list_zipf", pickle.dumps(self.image_pop_list_zipf))

        return self.image_pop_list_zipf


    def layers_(self, image):
        layers = self.imageinfo_map[image][2]
        # assert len(layers) > 0
        return layers

    def layer_size(self, digest):
        # assert digest in self.layerinfo_map
        return self.layerinfo_map[digest][1]

    def layer_image(self, digest):
        return self.layerinfo_map[digest][2]

    def layer_pop_list_(self):
        if self.layer_pop_list is None:
            self.layer_pop_list = [(k, v[0])
                                   for k, v in self.layerinfo_map.iteritems()]
        return self.layer_pop_list

    def layer_pull_time(self, digest):
        # a few config layers having zero size are not recorded in the trace
        # TODO: add on the fly layer pull generation based on some equation derived
        # TODO: from the layer pulling results
        assert digest in self.layerinfo_map
        # if digest not in self.layerpull_map:
        # return self.layer_pull_time_from_size(self.layer_size(digest))
        dl_time, reg_time = self.layerpull_map[digest]
        return dl_time + reg_time

    def layer_pull_time_from_size(self, layer_size, from_instance="m4.xlarge"):
        """Derive the reg and download latencies derived from the trace.
        Default is from m4.xlarge instance.
        TODO: add support for different instance results.
        """
        # return 0
        sep_value = 10 ** 5
        if layer_size <= sep_value:
            return 184
        else:
            return 2.50e-05 * layer_size + 367

    def layer_pull_time_stats(self):
        """Analyze the layer pull time results."""
        bin_flat, bin_linear = [], []
        sep_value = 10 ** 5
        for digest in self.layerpull_map:
            dl_time, reg_time = self.layerpull_map[digest]
            layer_size = self.layer_size(digest)
            entry = (layer_size, dl_time, reg_time, dl_time + reg_time)
            if layer_size <= sep_value:
                bin_flat.append(entry)
            else:
                bin_linear.append(entry)
        mean_flat_dl = np.mean([entry[1] for entry in bin_flat])
        mean_flat_reg = np.mean([entry[2] for entry in bin_flat])
        mean_flat_total = np.mean([entry[3] for entry in bin_flat])
        x, y = np.array([entry[0] for entry in bin_linear]), np.array([entry[3] for entry in bin_linear])
        z = np.polyfit(x, y, 1)
        # 117.27927927927928 67.1891891891892 184.46846846846847 [2.50382333e-05 3.67435258e+02]
        print(mean_flat_dl, mean_flat_reg, mean_flat_total, z)

    def layer_dl_time(self, digest):
        if digest not in self.layerpull_map:
            return 0
        return self.layerpull_map[digest][0]

    def layer_reg_time(self, digest):
        if digest not in self.layerpull_map:
            return 0
        return self.layerpull_map[digest][1]

    def filter_image(self, image_entry, filter):
        """Return true if any filter condition is met."""
        for field in filter._fields:
            if field == "max_size" and image_entry["size"] > getattr(filter, field):
                return True
            # add new fields below
        return False

    def stats_summary(self):
        assert len(self.layerinfo_map) == len(self.layerpull_map)
        image_items = self.imageinfo_map.items()
        layer_items = self.layerinfo_map.items()
        total_image_count = len(image_items)
        total_image_size = sum([i[1][1] for i in image_items])
        max_image_size = max([i[1][1] for i in image_items])
        total_layer_count = len(layer_items)
        total_layer_size = sum([l[1][1] for l in layer_items])
        max_layer_size = max([l[1][1] for l in layer_items])

        print("--> total image count: " + str(total_image_count))
        print("--> total image size: {} gb".format(int(total_image_size / gb)))
        print("--> max image size: {} gb".format(round(max_image_size / gb, 2)))
        print("--> avg. image size: {} mb".format(int(total_image_size / total_image_count / mb)))
        print("--> total layer count: " + str(total_layer_count))
        print("--> total layer size: {} gb".format(int(total_layer_size / gb)))
        print("--> max layer size: {} mb".format(int(max_layer_size / mb)))
        print("--> avg. layer size: {} mb".format(int(total_layer_size / total_layer_count / mb)))

        layer_set, size = set(), 0

        for image in image_items:
            layers = image[1][2]
            for l in layers:
                if l not in layer_set:
                    layer_set.add(l)
                    size += self.layer_size(l)
        # print(size, total_layer_size)
        assert size == total_layer_size
        print("--> stats checks pass")
        print(self.image_pop_list_zipf_()[:10])
        # print([(image[0], image[1][0]) for image in self.top_popular_images()])

    def dump_cdf(self):
        dots = sorted([(key, entry[1]) for key, entry in self.imageinfo_map.items()], key=lambda x: x[1])
        cdf = []
        for i, dot in enumerate(dots):
            cdf.append([str(dot[1]), str(i / len(dots))])
        with open(_result_path + "size_cdf.csv", "w") as f:
            for line in cdf:
                f.write(",".join(line) + "\n")

    def get_total_image_size(self):
        image_items = self.imageinfo_map.items()
        return sum([i[1][1] for i in image_items])

    def analyze_req_list(self, req_list):
        histogram = defaultdict(int)
        for req in req_list:
            image = req[0]
            histogram[image] += 1
        print(sorted(list(histogram.items()), key=lambda x: x[1], reverse=True)[:10])

    def top_popular_images(self, rank=100):
        return sorted(list(self.imageinfo_map.items()), key=lambda x: x[1][0], reverse=True)[:rank]


def main():
    from .utils import main_with_cmds
    tracer = Tracer()
    cmds = {
        "dump_redis": tracer.dump_to_redis,
        "load_redis": tracer.load_from_redis,
        "stat": tracer.stats_summary,
        "cdf": tracer.dump_cdf,
        "zipf": tracer.image_pop_list_zipf_,
    }
    main_with_cmds(cmds)

if __name__ == "__main__":
    main()
