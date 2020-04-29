#!/usr/bin/env python3
import random
import sys
import numpy as np
from .utils import mb, gb
from .utils import timed

"""
Schedule a request on a given set of nodes

states:
    - tracer, refer to the lookups in it
    - dep_th, dependency score threshold
"""

REJ_CONT_LIMIT = -2
REJ_STORE_LIMIT = -3
seed_pool = list(range(10000))
MAX_SUM_SIZE = 1 * gb
MIN_SUM_SIZE = 23 * mb


class Seeder():
    def __init__(self):
        self.reset()

    def reset(self):
        self.current = iter(seed_pool)

    def set_seed(self):
        try:
            return np.random.seed(next(self.current))
        except:
            self.reset()
            return np.random.seed(next(self.current))


class Scheduler():
    def __init__(self, tracer, dep_th=0.1):
        self.tr = tracer
        self.dep_th = dep_th
        self.seeder = Seeder()
        print("--> new scheduler init.")

    # @timed
    def schedule(self, req, nodes, sched="dep", lb_ratio=None):
        """
        scheduling policies:
            dep: select the highest dependency score
            dep-soft: select if the dependency score higher the a threshold
            kube: select if image is present, otherwise any permissible node
            monkey: select any permissible node

        before each schedule call, the nodes/nodes are shuffled
        """
        self.seeder.set_seed()
        self.visit_sequence = np.random.permutation(len(nodes))
        # print(self.visit_sequence)
        # random.shuffle(nodes)

        if sched == "dep":
            return self.dep_schedule(req, nodes, lb_ratio=lb_ratio)
        elif sched == "dep-soft":
            return self.dep_soft_schedule(req, nodes)
        elif sched == "kube":
            return self.kube_schedule(req, nodes, lb_ratio=lb_ratio)
        elif sched == "monkey":
            return self.monkey_schedule(req, nodes)
        else:
            print("--> error: unknown scheduling policy specified")
            sys.exit(1)

    def dep_schedule(self, req, nodes, lb_ratio=None):
        max_score, selected = -1, -1
        images = req[0]
        for i in self.visit_sequence:
            node = nodes[i]
            if self.node_conta_free(node) - len(images) < 0:
                # if self.node_conta_free(node) > 0:
                    # print(self.node_conta_free(node), len(images))
                if selected < 0:
                    selected = REJ_CONT_LIMIT
                continue
            total_size = sum([self.tr.image_size(i) for i in images])

            # note that here we assume the scheduler knows whether the node is able to free enough space
            if total_size > self.real_free_size(node):
                if selected < 0:
                    selected = REJ_STORE_LIMIT
                continue

            score = self.dep_score(images, node)

            if lb_ratio is not None:
                score_locality = self.scaled_score_locality(score)
                score_lb = (node[5] - node[4])/node[5] * 10
                score = lb_ratio * score_lb + (1 - lb_ratio) * score_locality

            if score > max_score:
                max_score = score
                selected = i
        return selected

    def scaled_score_locality(self, score):
        if score > MAX_SUM_SIZE:
            score = 10
        elif score < MIN_SUM_SIZE:
            score = 0
        else:
            score /= (MAX_SUM_SIZE - MIN_SUM_SIZE)
        return score

    def dep_soft_schedule(self, req, nodes):
        """TODO: fix the case of multiple image per req."""
        max_score, selected = -1, -1
        req_size = self.tr.image_size(req)
        for i in self.visit_sequence:
            node = nodes[i]
            if self.node_conta_free(node) <= 0:
                if selected < 0:
                    selected = REJ_CONT_LIMIT
                continue
            if self.tr.image_size(req) > self.real_free_size(node):
                if selected < 0:
                    selected = REJ_STORE_LIMIT
                continue
            score = self.dep_score(req, node)

            # the first node encountered has score >= threshold_score
            if score >= self.dep_th * req_size:
                selected = i
                break

            if score > max_score:
                max_score = score
                selected = i
        return selected

    def kube_schedule(self, req, nodes, lb_ratio=None):
        max_score, selected = -1, -1
        images = req[0]

        for i in self.visit_sequence:
            node = nodes[i]
            if self.node_conta_free(node) - len(images) < 0:
                if selected < 0:
                    selected = REJ_CONT_LIMIT
                continue

            total_size = sum([self.tr.image_size(i) for i in images])
            if total_size > self.real_free_size(node):
                if selected < 0:
                    selected = REJ_STORE_LIMIT
                continue

            node_images = node[0]

            score = sum([self.tr.image_size(i) for i in images if i in node_images])

            if lb_ratio is not None:
                score_locality = self.scaled_score_locality(score)
                score_lb = (node[5] - node[4])/node[5] * 10
                score = lb_ratio * score_lb + (1 - lb_ratio) * score_locality

            if score > max_score:
                max_score = score
                selected = i
        return selected

    def monkey_schedule(self, req, nodes):
        selected = -1
        images = req[0]
        for i in self.visit_sequence:
            node = nodes[i]
            if self.node_conta_free(node) - len(images) <= 0:
                if selected < 0:
                    selected = REJ_CONT_LIMIT
                continue
            total_size = sum([self.tr.image_size(i) for i in images])
            if total_size > self.real_free_size(node):
                if selected < 0:
                    selected = REJ_STORE_LIMIT
                continue
            selected = i
        return selected

    def dep_score(self, images, node):
        node_layers = node[1]

        score = 0
        for i in images:
            req_layers = self.tr.layers_(i)
            for l in req_layers:
                if l in node_layers:
                    score += self.tr.layer_size(l)
        return score

    def required_size(self, req, node, verbose=False):
        images, layers = node[0], node[1]
        if req in images:
            return 0
        size = 0

        for i in req:
            req_layers = self.tr.layers_(i)
            if verbose:
                print("real image size: ",
                      sum([self.tr.layer_size(l) for l in req_layers]))
            for l in req_layers:
                if l not in layers:
                    size += self.tr.layer_size(l)
        return size

    def real_free_size(self, node):
        """Given a node, compute how much space are free after eviction."""
        return node[7]

    def node_conta_free(self, node):
        return node[5] - node[4]

    def inc_node_conta(self, node, value=1):
        node[4] += value


def scheduler_test():
    pass


if __name__ == "__main__":
    scheduler_test()
