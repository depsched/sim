#!/usr/bin/env python3

import copy
import os
import pprint as pp
from collections import defaultdict

from . import simulator
from .utils import gb, banner, cmd

_dir_path = os.path.dirname(os.path.realpath(__file__))
_result_path = _dir_path + "/__plot__/__data__/"
_raw_result_path = _dir_path + "/__result__/"
_result_file = _result_path + "{exp_name}.csv"

default_params = {"sim_length": 1000,
                  "uniform": False,
                  "zipf": True,
                  "precached": False,
                  "pinned": False,
                  "cached_rank": 20,
                  "result_dir": _raw_result_path,
                  # node setup
                  "node_num": 200,
                  "store_size": 32 * gb,
                  "cont_cap": 16,
                  # request setup
                  "cont_length": 10,
                  "req_rate": 125,
                  "max_num_image": 1,
                  # container reuse
                  "reuse": False,
                  "hot_duration": 20,
                  # sim_length is used to here to denote total number of requests
                  "rerun": False,
                  # scheduling policies
                  "policies": ["dep", "kube", "monkey"],
                  "dep_th": 0.1,
                  # eviction
                  "evict": True,
                  "evict_th": 0.1,
                  "evict_policy": "dep-lfu",
                  "evict_dep": False,
                  # delay scheduling params
                  "delay_sched": False,
                  "provision_gap": 2,
                  "delay": 1,
                  # adaptive scoring
                  "lb_ratio": 0,
                  # runtime scaling
                  "num_puller": 1,
                  }


"""Scheduling mechanisms."""


def _exp_delay_sched(mode):
    """Factor analysis for delay scheduling."""
    # add per node queue for delay scheduling
    exp_name = "exp_delay_sched_" + mode
    raw_result_dir = _raw_result_path + "run_delay_sched/" + mode + "/{}/"
    s = simulator.Simulator()
    params = copy.deepcopy(default_params)
    observer = ExpObserver(exp_name, params)

    params = copy.deepcopy(default_params)
    # experiment specific setup
    params["policies"] = ["dep"]
    params["req_rate"] = 140
    results_in_row = []

    # experiment routines; first run
    for run, delay_sched in enumerate([False, True]):
        params["delay_sched"] = delay_sched
        params["result_dir"] = raw_result_dir.format(delay_sched)
        if run > 1:
            params["rerun"] = True
        results = s.sim(**params)
        results_in_row.append(
            tuple([params["delay_sched"]] + results["mean_provision_lat"] + results["mean_startup_lat"]))

    return params


def _exp_evict_dep(mode):
    """Factor analysis for eviction policies."""
    exp_name = "exp_evict_dep_" + mode
    raw_result_dir = _raw_result_path + "run_evict_dep/" + mode + "/{}/"
    s = simulator.Simulator()
    params = copy.deepcopy(default_params)
    observer = ExpObserver(exp_name, params)

    # experiment specific setups
    evict_enabled = [False, True]
    params.update({
                   "policies": ["dep"],
                   "sim_length": 2000,
                   "node_num": 200,
                   "store_size": 32 * gb,
                   })

    for var in evict_enabled:
        params["evict_dep"] = var
        params["result_dir"] = raw_result_dir.format(var)
        observer.observe(s.sim(**params), key=str(var),
                         custom_mix=("mean_provision_lat", "mean_startup_lat"))
        params["rerun"] = True

    observer.save(omit={}, printout=True, dump_params=True)
    return params


def _exp_ada(mode):
    exp_name = "exp_ada" + mode
    raw_result_dir = _raw_result_path + "run_ada/" + mode + "/{}/"
    s = simulator.Simulator()
    params = copy.deepcopy(default_params)
    observer = ExpObserver(exp_name, params)

    # experiment specific setup
    if mode == "uniform":
        return
    elif mode == "zipf":
        lb_ratios = [0, 0.25, 0.5, 0.75, 1]
    else:
        return

    # experiment routines
    for l in lb_ratios:
        params["lb_ratio"] = l
        params["result_dir"] = raw_result_dir.format(l)
        observer.observe(s.sim(**params), key=l)
        params["rerun"] = True
    return params


"""Request configurations."""


def _exp_num_image(mode):
    exp_name = "exp_num_image_" + mode
    raw_result_dir = _raw_result_path + "run_num_image/" + mode + "/{}/"
    s = simulator.Simulator()
    params = copy.deepcopy(default_params)
    observer = ExpObserver(exp_name, params)

    # experiment specific setup
    if mode == "uniform":
        return
    elif mode == "zipf":
        max_num_image_range = [1, 3, 5]
        rates = [90, 30, 16]
    else:
        return

    # experiment routines
    for i, r in zip(max_num_image_range, rates):
        params["max_num_image"] = i
        params["req_rate"] = r
        params["result_dir"] = raw_result_dir.format(r)
        observer.observe(s.sim(**params), key=r)
        params["rerun"] = True
    return params


def _exp_req_rate(mode):
    exp_name = "exp_req_rate_" + mode
    raw_result_dir = _raw_result_path + "run_req_rate/" + mode + "/{}/"
    s = simulator.Simulator()
    params = copy.deepcopy(default_params)
    observer = ExpObserver(exp_name, params)

    # experiment specific setup
    if mode == "uniform":
        rates = [30, 50, 70, 90, 110, 130]
    elif mode == "zipf":
        rates = [40, 60, 80, 100, 125, 150]
    else:
        rates = [60, 100, 140, 180, 215, 240]

    # experiment routines
    for r in rates:
        params["req_rate"] = r
        params["result_dir"] = raw_result_dir.format(r)
        observer.observe(s.sim(**params), key=r)
        params["rerun"] = True
    return params


# system eval cdf, with zipf
def _exp_cont_length(mode):
    s = simulator.Simulator()
    result_dir = "./result/run_cont_length/" + mode + "/{}/"

    params = copy.deepcopy(default_params)
    lengths = [5, 10, 20, 30, 40]
    # experiment specific setup

    # experiment routines
    params["cont_length"] = lengths[0]
    params["result_dir"] = result_dir.format(lengths.pop(0))

    s.sim(**params)

    for l in lengths:
        params["cont_length"] = l
        params["result_dir"] = result_dir.format(l)
        params["rerun"] = True
        s.sim(**params)

    return params


"""Cluster configurations."""


def _exp_cluster_size(mode):
    """Fixed node cache and cap size, increasing cluster size."""
    exp_name = "exp_cluster_size_" + mode
    raw_result_dir = _raw_result_path + "run_cluster_size/" + mode + "/{}/"
    s = simulator.Simulator()
    params = copy.deepcopy(default_params)
    observer = ExpObserver(exp_name, params)

    # experiment specific setups
    cluster_sizes = [25, 50, 100, 200, 400, 800, 1000]

    if mode == "pop":
        provision_overhead = 13
    elif mode == "uniform":
        provision_overhead = 25
    else:
        provision_overhead = 20

    for var in cluster_sizes:
        params["node_num"] = var
        params["result_dir"] = raw_result_dir.format(var)
        # req_rate * (max_cont_length/2 + 2) ~= node_num * cont_cap;
        # the provision latency is estimated as 25)
        params["req_rate"] = int(var * params["cont_cap"] // (params["cont_length"] / 2 + provision_overhead))

        observer.observe(s.sim(**params), key=var)
        params["rerun"] = True

    observer.save(omit={}, printout=True, dump_params=True)
    return params


def _exp_store_size(mode):
    """Fixed cluster size, increasing node cache size and cap."""
    exp_name = "exp_store_size_" + mode
    raw_result_dir = _raw_result_path + "run_store_size/" + mode + "/{}/"
    s = simulator.Simulator()
    params = copy.deepcopy(default_params)
    observer = ExpObserver(exp_name, params)

    # experiment specific setups
    store_sizes = [16, 24, 32, 48, 64]
    for var in store_sizes:
        params["store_size"] = var * gb
        params["result_dir"] = raw_result_dir.format(var)

        observer.observe(s.sim(**params), key=var)
        params["rerun"] = True

    observer.save(omit={}, printout=True, dump_params=True)
    return params


def _exp_pool(uniform):
    """Fixed cluster size, increasing node cache size and cap."""
    exp_name = "exp_pool_" + ("uniform" if uniform else "pop")
    raw_result_dir = _raw_result_path + "run_pool/" + ("uniform/{}/" if uniform else "pop/{}/")
    s = simulator.Simulator()
    params = copy.deepcopy(default_params)
    observer = ExpObserver(exp_name, params)

    # experiment specific setups
    # (cluster size, node cap/cache)
    configs = [(64, 4), (32, 8), (16, 16), (8, 32), (4, 64)]
    params.update({"uniform": uniform,
                   })

    for var in configs:
        params["node_num"] = var[0]
        params["cont_cap"] = var[1]
        params["store_size"] = var[1] * gb
        if var[1] < 10:
            params["evict_th"] = 1 / var[1]
        params["result_dir"] = raw_result_dir.format(var[0])
        # req_rate * (max_cont_length/2 + 2) ~= node_num * cont_cap;
        # the provision latency is estimated as 15)

        observer.observe(s.sim(**params), key=var[0])
        params["rerun"] = True

    observer.save(omit={}, printout=True, dump_params=True)
    return params


def _exp_precached(uniform):
    """Fixed cluster size, increasing node cache size and cap."""
    exp_name = "exp_precached_" + ("uniform" if uniform else "pop")
    raw_result_dir = _raw_result_path + "run_precached/" + ("uniform/{}/" if uniform else "pop/{}/")
    s = simulator.Simulator()
    params = copy.deepcopy(default_params)
    observer = ExpObserver(exp_name, params)

    # experiment specific setups
    precached = [False, True]
    params.update({"uniform": uniform,
                   })

    for var in precached:
        params["precached"] = var
        params["result_dir"] = raw_result_dir.format(var)

        observer.observe(s.sim(**params), key=var)
        params["rerun"] = True

    observer.save(omit={}, printout=True, dump_params=True)
    return params


def _exp_cont_cap(uniform):
    # result_dir = "./result/run_cluster_size/" + "uniform/{}/" if uniform else "pop/{}"
    s = simulator.Simulator()
    if uniform:
        result_dir = "./result/run_cont_cap/uniform/{}/"
    else:
        result_dir = "./result/run_cont_cap/pop/{}/"
    caps = [96, 64, 48, 16, 8]

    params = copy.deepcopy(default_params)
    # experiment specific setup
    # params["req_rate"] = 10

    # experiment routines
    params["uniform"] = uniform
    params["cont_cap"] = caps[0]
    params["result_dir"] = result_dir.format(caps.pop(0))

    s.sim(**params)

    for c in caps:
        params["cont_cap"] = c
        params["result_dir"] = result_dir.format(c)
        params["rerun"] = True
        s.sim(**params)

    return params
    # exp_name = "exp_store_size_" + mode
    # raw_result_dir = _raw_result_path + "run_store_size/" + mode + "/{}/"
    # s = simulator.Simulator()
    # params = copy.deepcopy(default_params)
    # observer = ExpObserver(exp_name, params)
    #
    # # experiment specific setups
    # store_sizes = [16, 24, 32, 48, 64]
    # for var in store_sizes:
    #     params["store_size"] = var * gb
    #     params["result_dir"] = raw_result_dir.format(var)
    #
    #     observer.observe(s.sim(**params), key=var)
    #     params["rerun"] = True
    #
    # observer.save(omit={}, printout=True, dump_params=True)
    # return params


"""Latency CDF."""


def _exp_lat_cdf(mode):
    exp_name = "exp_lat_cdf_" + mode
    raw_result_dir = _raw_result_path + "run_lat_cdf/" + mode + "/"
    s = simulator.Simulator()
    params = copy.deepcopy(default_params)
    observer = ExpObserver(exp_name, params)

    # experiment routines
    params["policies"] = ["dep", "kube", "monkey"]
    params["result_dir"] = raw_result_dir
    observer.observe(s.sim(**params), key="")
    return params


def exp_routine(exp, uniform=False, pop=False, zipf=True):
    _clear(exp)
    exp_func = globals()["_exp_" + exp]

    if uniform:
        banner("Start of {}-uniform".format(exp))
        default_params.update({"uniform": True,
                               "zipf": False,
                               "req_rate": 110})
        params = exp_func("uniform")
    if zipf:
        banner("Start of {}-zipf".format(exp))
        default_params.update({"zipf": True,
                               "req_rate": 125})
        params = exp_func("zipf")
    if pop:
        banner("Start of {}-pop".format(exp))
        default_params.update({"uniform": False,
                               "zipf": False,
                               "req_rate": 215})
        params = exp_func("pop")

    banner("End of {}".format(exp))
    _dump_params(exp, params)
    cmd("chmod -R 666 {}".format(_result_path))


def _dump_params(exp="null", params=None):
    if exp in params:
        params.pop(exp)
    if "store_size" in params:
        params["store_size"] = str(params["store_size"] / gb) + " gb"
    with open(_raw_result_path + "setup_{}.txt".format(exp), "w") as f:
        f.write(str(params))


def _clear(exp=None):
    if exp is None:
        # cmd("rm -rf ./plot/")
        cmd("rm -rf {}".format(_raw_result_path))
    else:
        cmd("rm -f {}meta_{}.txt".format(_raw_result_path, exp))
        cmd("rm -rf {}run_{}".format(_raw_result_path, exp))


class ExpObserver:
    """Records and saves the simulation results."""

    def __init__(self, exp_name, params):
        self.exp_name = exp_name
        self.params = params
        self.metrics = defaultdict(list)

    def observe(self, results, *,
                key=None, omit=None, custom_mix=None):
        """Record and store once. Results are indexed by the
        metric name, stored as a list of tuples, each tuple
        represents a row."""
        if custom_mix:
            # custom mix is a tuple, e.g., (mean_startup_lat, mean_provision_lat)
            entry = [key] if key else []
            for metric_name in custom_mix:
                for result in results[metric_name]:
                    entry.append(result)
            self.metrics[custom_mix].append(entry)

        for metric_name, values in results.items():
            if not (omit and metric_name in omit):
                if key:
                    values = [key] + values
                self.metrics[metric_name].append(tuple(values))

    def save(self, *, omit=None, printout=False, dump_params=False):
        for metric, rows in self.metrics.items():
            if not (omit and metric in omit):
                rows = self._flatten_rows(rows)
                exp_name = "_".join([self.exp_name, str(metric)])
                if printout:
                    print("\n--> toggled results for {}: \n".format(exp_name))
                    print(rows)

                if dump_params:
                    with open(_result_file.format(exp_name=exp_name + "_params"), "w") as f:
                        f.write(pp.pformat(self.params))

                with open(_result_file.format(exp_name=exp_name), "w") as f:
                    f.write(rows)

    def _flatten_rows(self, rows):
        """Given a list of tuples where each tuple represents
        a row, returns a string where each row is on a separate
        line."""
        for i, row in enumerate(rows):
            rows[i] = ",".join(str(j) for j in row)
        return "\n".join(rows)


def _exp_all():
    exp_routine("req_rate")
    exp_routine("cluster_size")
    exp_routine("store_size")
    # exp_routine("pool")


def main():
    from .utils import main_with_cmds, depsched_intro
    depsched_intro()

    cmds = {"cluster": lambda: exp_routine("cluster_size"),
            "store": lambda: exp_routine("store_size"),
            "pool": lambda: exp_routine("pool"),
            "evict": lambda: exp_routine("evict_dep"),
            "precache": lambda: exp_routine("precached"),
            "cdf": lambda: exp_routine("lat_cdf", False, False, True),
            "delay": lambda: exp_routine("delay_sched"),
            "util": lambda: exp_routine("req_rate"),
            "num": lambda: exp_routine("num_image"),
            "ada": lambda: exp_routine("ada"),
            "all": _exp_all,
            "clean": lambda: cmd("rm -rf {}*".format(_raw_result_path))
            }

    main_with_cmds(cmds)


if __name__ == "__main__":
    main()
