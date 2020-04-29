import requests
import cloudpickle as pickle
import time
import subprocess
import uuid
from kubernetes import config
from kubernetes.client import Configuration
from kubernetes.client.apis import core_v1_api
from .utils import cmd, get_free_tcp_port
import numpy as np
import asyncio

"""
Deprecated - use pywren/executor.py instead
"""

repo_address = "238764668013.dkr.ecr.us-west-1.amazonaws.com/"
handler_port = "7777"


def init():
    global api
    config.load_kube_config()
    c = Configuration()
    c.assert_hostname = False
    # Configuration.set_default(c)
    api = core_v1_api.CoreV1Api()


def simple_add(a, b):
    return a + b


def simple_import():
    import scipy


func_image_map = {
    "simple_add": "wren-opt",
    # "simple_add": "wren-ml",
    "simple_add_opt": "wren-ml",
    "warm": "wren-default"
}


def warm(num=10):
    print("warming..")
    from .cmd import save, replace_origin, replace
    save()
    replace_origin()
    for _ in range(num):
        execute(simple_add, [1, 2], image="wren-default")
    replace()


def restart_cluster():
    cmd("python3 -m build.kube.cluster down")
    cmd("python3 -m build.kube.cluster up")


def exp_test():
    print(execute(simple_add, [1, 2]))


def exp_simple_add():
    init()
    from .cmd import replace, replace_image_focused, replace_unfocused
    from .cmd import replace_image, replace_origin, save
    num_runs = 10
    concurrency = 5
    schedulers = ["kube", "image-focused", "kube-unfocused",
                  "image", "origin"]
    schedulers = ["origin"]
    schedulers = ["kube"]
    schedulers = ["image"]
    row_exp_repeated = []
    row_exp_concurrency = []
    row_exp_change = []

    # keep a copy of the original manifest
    # save()

    # warm the cache
    # replace_origin()
    warm()

    for s in schedulers:
        print("running {}..".format(s))
        if s == "kube":
            replace()
        elif s == "image-focused":
            replace_image_focused()
        elif s == "kube-unfocused":
            replace_unfocused()
        elif s == "image":
            replace_image()
        else:
            replace_origin()
        # wait for scheduler gets ready
        time.sleep(3)

        # warm
        execute(simple_add, [1, 2])
        time.sleep(1)

        # start runs; first with repeated exp
        latencies = []
        for _ in range(num_runs):
            latencies.append(execute(simple_add, [1, 2]))
        # std = np.std(latencies)
        mean = np.mean(latencies)
        # record = [np.round(mean,2), np.round(mean - std,2), np.round(mean + std,2)]; print(record)
        record = [np.round(mean, 2)]; print(record)
        row_exp_repeated.extend(record)

        # concurrency exp
        # std = np.std(latencies)
        # mean = np.mean(latencies)
        # record = [np.round(mean,2), np.round(mean - std,2), np.round(mean + std,2)]; print(record)
        # record = [np.round(mean, 2)]; print(record)
        # row_exp_concurrency.extend(record)

        # change exp
        latencies = []
        for _ in range(1):
            latencies.append(execute(simple_add, [1, 2], image=func_image_map["simple_add_opt"]))
        # std = np.std(latencies)
        mean = np.mean(latencies)
        # record = [np.round(mean,2), np.round(mean - std,2), np.round(mean + std,2)]; print(record)
        record = [np.round(mean, 2)]; print(record)
        row_exp_change.extend(record)

        # restart_cluster()
    print(",".join(list(map(str, row_exp_repeated))))
    print(",".join(list(map(str, row_exp_concurrency))))
    print(",".join(list(map(str, row_exp_change))))


def execute(func, args, image=None):
    start = time.time()
    data = pickle.dumps({
        "func": func,
        "args": args,
    })
    # k8s object name should not contain underscore
    func_name = func.__name__
    task_id = func_name.replace("_", "-") + "-" + str(uuid.uuid4())
    image = image if image else func_image_map[func_name]

    # launch pod
    pod_name = launch(task_id, image)

    # set up port forwarding
    local_port = str(get_free_tcp_port())
    mapped_port = "{}:{}".format(local_port, handler_port)
    port_forward(pod_name, mapped_port)

    # submit task
    resp = requests.get("http://127.0.0.1:{}".format(local_port), data=data)
    result = pickle.loads(resp.content)
    latency = time.time() - start
    print("result: {} time: {}".format(result, latency))
    return latency


async def execute_async(func, args, image=None):
    start = time.time()
    data = pickle.dumps({
        "func": func,
        "args": args,
    })
    # k8s object name should not contain underscore
    func_name = func.__name__
    task_id = func_name.replace("_", "-") + "-" + str(uuid.uuid4())
    image = image if image else func_image_map[func_name]

    # launch pod
    print("submitted an async task..")
    pod_name = await asyncio.coroutine(launch)(task_id, image)

    # set up port forwarding
    local_port = str(get_free_tcp_port())
    mapped_port = "{}:{}".format(local_port, handler_port)
    port_forward(pod_name, mapped_port)

    # submit task
    resp = requests.get("http://127.0.0.1:{}".format(local_port), data=data)
    result = pickle.loads(resp.content)
    latency = time.time() - start
    print("result: {} time: {}".format(result, latency))
    return latency


def port_forward(pod, port_map, namespace="default"):
    print(pod, port_map)
    cmdstr = "kubectl port-forward {} {} > /dev/null 2>&1 & sleep 0.3".format(pod, port_map)
    cmd(cmdstr)


def launch(name, image, scheduler="default-scheduler"):
    """Synchronous launch pod until ready."""
    if ":" not in image:
        image += ":latest"
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
                'image': repo_address + image,
                'name': 'sleep',
                "args": [
                ]
            }]
        }
    }
    resp = api.create_namespaced_pod(body=pod_manifest,
                                     namespace='default',
                                     )
    while True:
        resp = api.read_namespaced_pod(name=name,
                                       namespace='default')
        if resp.status.phase != 'Pending':
            break
        time.sleep(0.5)
    return name


def execute_local(local_port=7777):
    start = time.time()
    data = pickle.dumps({
        "func": simple_add,
        "args": [1, 2],
    })
    resp = requests.get("http://127.0.0.1:{}".format(local_port), data=data)
    result = pickle.loads(resp.content)
    print("result: {} time: {}".format(result, time.time() - start))


def wren():
    init()
    exp_simple_add()


def main():
    init()
    latencies = []
    for _ in range(1):
        latencies.append(execute(simple_add, [1,2]))
    print("mean: {}".format(np.mean(latencies)))
    # wren()


if __name__ == "__main__":
    main()
