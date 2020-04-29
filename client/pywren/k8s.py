import cloudpickle as pickle
from collections import defaultdict
import time
import requests
import uuid
import asyncio
import numpy as np
import ray
import random
import pprint as pp

from kubernetes import config
from kubernetes.client import Configuration
from kubernetes.client.apis import core_v1_api

from scipy.ndimage.filters import convolve as convolveim
from cvxopt import matrix, solvers

from ..cmd import replace, replace_image_focused, replace_unfocused, replace_layer
from ..cmd import replace_image, replace_origin, save
from ..utils import cmd, get_free_tcp_port
from .. import k8s

repo_address = "238764668013.dkr.ecr.us-west-1.amazonaws.com"
handler_port = "7777"


def init():
    global api
    config.load_kube_config()
    c = Configuration()
    c.assert_hostname = False
    # Configuration.set_default(c)
    api = core_v1_api.CoreV1Api()


@ray.remote
def _clear_image(worker, image):
    cmd("ssh -o 'StrictHostKeyChecking no' admin@%s 'sudo docker rmi %s'" % (worker, image))


def clear_images_from_workers(names, tags=("latest",)):
    wip = k8s.get_worker_external_ip_map()
    worker_ips = list()

    for n, ls in k8s.get_node_map(selectors=k8s.default_excl_selectors, reverse=True).items():
        if "node-role.kubernetes.io/master" not in ls:
            worker_ips.append(wip[n])

    futures = list()
    for w in worker_ips:
        for n in names:
            for t in tags:
                futures.append(_clear_image.remote(w, n + ":" + t))
    ray.get(futures)
    print("images cleared")


"""Functions"""


def simple_add():
    return 1 + 1


def conv():
    a = np.random.rand(*(100, 100, 400))
    b = np.random.rand(*(10, 10, 10))

    conv1 = convolveim(a, b, mode='constant')
    # conv2 = convolvesig(a,b, mode = 'same')
    return conv1  # FLOPS / (t2-t1)


def cvx_0():
    # Create two scalar optimization variables.
    A = matrix([[-1.0, -1.0, 0.0, 1.0], [1.0, -1.0, -1.0, -2.0]])
    b = matrix([1.0, -2.0, 0.0, 4.0])
    c = matrix([2.0, 1.0])
    sol = solvers.lp(c, A, b)
    return sol


def cvx():
    # Figures 6.8-10, pages 313-314
    # Quadratic smoothing.

    from math import pi
    from cvxopt import blas, lapack, matrix, sin, mul, normal

    n = 4000
    t = matrix(list(range(n)), tc='d')
    ex = 0.5 * mul(sin(2 * pi / n * t), sin(0.01 * t))
    corr = ex + 0.05 * normal(n, 1)

    # A = D'*D is an n by n tridiagonal matrix with -1.0 on the
    # upper/lower diagonal and 1, 2, 2, ..., 2, 2, 1 on the diagonal.
    Ad = matrix([1.0] + (n - 2) * [2.0] + [1.0])
    As = matrix(-1.0, (n - 1, 1))

    nopts = 50
    deltas = -10.0 + 20.0 / (nopts - 1) * matrix(list(range(nopts)))
    cost1, cost2 = [], []
    for delta in deltas:
        xr = +corr
        lapack.ptsv(1.0 + 10 ** delta * Ad, 10 ** delta * As, xr)
        cost1 += [blas.nrm2(xr - corr)]
        cost2 += [blas.nrm2(xr[1:] - xr[:-1])]

    # Find solutions with ||xhat - xcorr || roughly equal to 8.0, 3.1, 1.0.
    time.sleep(1)
    mv1, k1 = min(zip([abs(c - 8.0) for c in cost1], range(nopts)))
    xr1 = +corr
    lapack.ptsv(1.0 + 10 ** deltas[k1] * Ad, 10 ** deltas[k1] * As, xr1)
    mv2, k2 = min(zip([abs(c - 3.1) for c in cost1], range(nopts)))
    xr2 = +corr
    lapack.ptsv(1.0 + 10 ** deltas[k2] * Ad, 10 ** deltas[k2] * As, xr2)
    mv3, k3 = min(zip([abs(c - 1.0) for c in cost1], range(nopts)))
    xr3 = +corr
    lapack.ptsv(1.0 + 10 ** deltas[k3] * Ad, 10 ** deltas[k3] * As, xr3)


def ml():
    import numpy as np
    from sklearn.neighbors import LocalOutlierFactor

    np.random.seed(42)

    # Generate train data
    X = 0.3 * np.random.randn(1000, 2)
    # Generate some abnormal novel observations
    X_outliers = np.random.uniform(low=-4, high=4, size=(20, 2))
    X = np.r_[X + 2, X - 2, X_outliers]

    # fit the model
    clf = LocalOutlierFactor(n_neighbors=20)
    y_pred = clf.fit_predict(X)
    y_pred_outliers = y_pred[200:]

    # plot the level sets of the decision function
    size = 500
    xx, yy = np.meshgrid(np.linspace(-5, 5, size), np.linspace(-5, 5, size))
    Z = clf._decision_function(np.c_[xx.ravel(), yy.ravel()])
    Z = Z.reshape(xx.shape)
    return Z


def simple_import():
    import scipy


func_image_map = {
    "cvx": "wren-opt",
    "simple_add": "wren-minimal",
    "ml": "wren-ml",
    "conv": "wren-default"
}


def warm(num=10):
    print("warming..")
    save()
    replace_origin()
    for _ in range(num):
        execute(simple_add, [1, 2], image="wren-default")
    replace()


def restart_cluster():
    cmd("python3 -m build.kube.cluster down")
    cmd("python3 -m build.kube.cluster up")


# executors
def port_forward(pod, port_map, namespace="default"):
    print(pod, port_map)
    # cmdstr = "kubectl port-forward {} {} > /dev/null 2>&1 &".format(pod, port_map)
    cmdstr = "kubectl port-forward {} {} &".format(pod, port_map)
    cmd(cmdstr)
    time.sleep(1)


def launch(name, image, k8s_client=None, scheduler="default-scheduler"):
    """Synchronous launch pod until ready."""
    # if ":" not in image:
    #     tag = np.random.choice(tags, 1)[0]
    #     image = image + ":" + tag

    pod_manifest = {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {
            'name': name
        },
        'spec': {
            "schedulerName": scheduler,
            "restartPolicy": "Never",
            'containers': [{
                'image': repo_address + "/" + image,
                'imagePullPolicy': "IfNotPresent",
                # 'resources': {
                #     'requests': {
                #         'cpu': "1",
                #         'memory': "1000Mi",
                #     },
                # },
                'name': 'sleep',
                "args": [
                ]
            }]
        }
    }

    k8s_client = core_v1_api.CoreV1Api() if k8s_client is None else k8s_client
    resp = k8s_client.create_namespaced_pod(body=pod_manifest,
                                            namespace='default',
                                            )
    while True:
        resp = k8s_client.read_namespaced_pod(name=name,
                                              namespace='default')
        # print(resp.status.phase)
        if resp.status.phase == 'Running':
            break
            # print(resp.status.phase)
        time.sleep(1)
    return name


"""Experiments"""


@ray.remote
def ray_exec(func, args, tid, tag=None, tags=("latest",), image=None):
    start = time.time()
    data = pickle.dumps({
        "func": func,
        "args": args,
    })
    # k8s object name should not contain underscore
    func_name = func.__name__
    print("executing:", func_name)
    task_id = func_name.replace("_", "-") + "-" + str(uuid.uuid4())
    image = image if image else func_image_map[func_name]

    from kubernetes import config, client
    config.load_kube_config()
    k8s_client = core_v1_api.CoreV1Api()

    # launch pod
    tag = np.random.choice(tags, 1)[0] if tag is None else tag
    image = image + ":" + tag

    pod_name = launch(task_id, image, k8s_client)

    # set up port forwarding
    local_port = str(get_free_tcp_port())
    mapped_port = "{}:{}".format(local_port, handler_port)
    port_forward(pod_name, mapped_port)

    # submit task
    try:
        resp = requests.get("http://127.0.0.1:{}".format(local_port), data=data)
        result = pickle.loads(resp.content)
        latency = time.time() - start

        k8s_client.delete_namespaced_pod(pod_name, namespace="default", body=client.V1DeleteOptions())
        # print("result: {} time: {}".format(result, latency))
        return {
            "func": func_name,
            "lat": latency,
            "tid": tid,
        }
    except:
        return None


def exp_version():
    for n in range(11):
        tags = ["latest"] + ["v" + str(i) for i in range(1, n, 2)]
        results = exp_mix(task_funcs=[ml])


def exp_mix(num_round=60, interval=1, load=1, task_funcs=None):
    random.seed(42)
    np.random.seed(42)
    ray.init(num_cpus=8)

    cmd("pkill kubectl")
    cmd("kubectl delete pods --all")

    tags = ["latest"] + ["v" + str(i) for i in range(1, 3, 1)]
    clear_images_from_workers([repo_address + "/" + i for i in func_image_map.values()], tags)

    task_funcs = [simple_add, cvx, conv, ml] if task_funcs is None else task_funcs
    futures, tid = list(), 0

    # warm up
    for t in task_funcs:
        for tag in tags:
            if random.random() > 0.6:
                futures += [ray_exec.remote(t, [], 0, tag=tag, tags=tags)]

    ray.get(futures)
    time.sleep(20)

    # actual tasks
    futures = list()
    for _ in range(num_round):
        num_task = np.random.poisson(load)
        tf = np.random.choice(task_funcs, size=num_task, replace=True)
        futures += [ray_exec.remote(t, [], tid + i, tags=tags) for i, t in enumerate(tf)]
        tid = len(futures)

        ray.wait(futures, timeout=0)
        time.sleep(interval)

    results = ray.get(futures)
    # results = ray.get(futures)

    # parsing the results
    summary = defaultdict(list)
    for r in results:
        if r is not None:
            summary[r["func"]].append(
                (r["tid"], r["lat"] - 1.5 if "add" in r["func"] else r["lat"]))  # the client wait is 1.5 second

    # organize the results
    for k, v in summary.items():
        summary[k] = sorted(v, key=lambda x: x[0])
    pp.pprint(dict(summary))
    return summary


def main():
    # replace_image_focused()
    # replace_origin()
    # replace_layer()
    # start = time.time()
    # print(time.time() - start)
    exp_mix()
    # exp_version()
    # cvx()
    exit()

    # latencies = []
    # for _ in range(1):
    # latencies.append(execute(simple_add, []))
    # latencies.append(execute(cvx, []))
    # latencies.append(execute(conv, []))
    # latencies.append(execute(ml, []))
    # print("mean: {}".format(np.mean(latencies)))


if __name__ == "__main__":
    # ml()
    # conv()
    main()
