#!/usr/bin/env python3

from __future__ import print_function
from collections import defaultdict
from .schedule import REJ_CONT_LIMIT, REJ_STORE_LIMIT
from .utils import gb, mb
import os

import numpy as np

_dir_path = os.path.dirname(os.path.realpath(__file__))
_result_path = _dir_path + "/__plot__/__data__/"

class Telemetry:
    def __init__(self, tracer=None, verbose=0):
        self.verbose = verbose
        self.reset()
        self.tr = tracer

    def reset(self):
        self.rej_cont_cnt = 0
        self.rej_store_cnt = 0
        self.gc_cnt = 0
        self.gc_size = 0
        self.admit_reqs = []
        self.node_snaps = defaultdict(list)

    def report_rej(self, code):
        if code == REJ_CONT_LIMIT:
            self.rej_cont_cnt += 1
        elif code == REJ_STORE_LIMIT:
            self.rej_store_cnt += 1
        else:
            print("--> unknown telemetry code")
            sys.exit(1)

    def report_gc(self, size=0):
        self.gc_size += size
        self.gc_cnt += 1

    def report_spread(self, image, nodes):
        pass

    def report_req(self, req):
        self.admit_reqs.append(req)

    def tel_util(self, total_provision_time=0):
        total_runtime, total_provision_time = 0, total_provision_time/1000
        for req in self.admit_reqs:
            total_runtime += len(req[0]) * req[1]
        total_cont_time = self.tr.get_metric("node_num") * \
                self.tr.get_metric("duration") * \
                self.tr.get_metric("cont_cap")
        util = (total_runtime+total_provision_time)/total_cont_time
        self.tr.set_metric("util", util)
        return util

    def tel_rej(self):
        total_rej = self.rej_cont_cnt + self.rej_store_cnt
        total_req = self.tr.get_metric(
            "sim_length") * self.tr.get_metric("req_rate")
        ratio = float(total_rej) / total_req
        self.tr.set_metric("cont_limit_rej", self.rej_cont_cnt)
        self.tr.set_metric("store_limit_rej", self.rej_store_cnt)
        self.tr.set_metric("rej_ratio", round(ratio, 3))
        print("--> Round all: cont_limit_rej: " + str(self.rej_cont_cnt))
        print("--> Round all: store_limit_rej: " + str(self.rej_store_cnt))

    def tel_nodes(self, nodes, rnd=0, verbose=0):
        verbose = max(verbose, self.verbose)
        image_histo = []
        layer_histo = []
        layer_store_histo = []
        contn_histo = []
        prefix = "--> Round " + str(rnd) + ": "
        for i in range(len(nodes)):
            images, layers = nodes[i][0], nodes[i][1]
            image_histo.append(len(images))
            layer_histo.append(len(layers))
            layer_store_histo.append((nodes[i][3] - nodes[i][2]) / mb)

        print("".join(["="] * 30))
        if verbose >= 1:
            print(image_histo, "\n",
                  prefix + "mean image #:",
                  sum(image_histo) / len(image_histo))
            print(layer_store_histo, "\n",
                  prefix + "mean residual store size:",
                  sum(layer_store_histo) / len(layer_store_histo))
        else:
            print(prefix + "mean image #:",
                  sum(image_histo) / len(image_histo))
            print(prefix + "mean layer #:",
                  sum(layer_histo) / len(layer_histo))
            print(prefix + "mean residual store size:",
                  sum(layer_store_histo) / len(layer_store_histo))
        return

    def tel_node_snap(self, nodes, image=False):
        for i, n in enumerate(nodes):
            num_image = len(n[0])
            real_free_space = n[-1] / mb
            free_space = (n[3] - n[2]) / mb
            if i not in self.node_snaps:
                self.node_snaps[i] = {
                    "num_image": [],
                    "real_free_space": [],
                    "free_space": [],
                }
            self.node_snaps[i]["num_image"].append(num_image)
            self.node_snaps[i]["real_free_space"].append(real_free_space)
            self.node_snaps[i]["free_space"].append(free_space)

    def reduce_node_snap(self):
        mean_num_image, mean_real_free_space, mean_free_space = [], [], []
        for _, s in self.node_snaps.items():
            mean_num_image.append(np.mean(s["num_image"]))
            mean_free_space.append(np.mean(s["free_space"]))
            mean_real_free_space.append(np.mean(s["free_space"]))
        print("mean image: ", np.mean(mean_num_image))
        print("mean real free space: ", np.mean(mean_real_free_space))
        print("mean free space: ", np.mean(mean_free_space))

    def tel_blank(self):
        print("\n")

    def tel_gc(self):
        print("--> Round all: layer_gc: {}; size: {}mb".format(str(self.gc_cnt),
                                                               str(round(self.gc_size / mb))))

