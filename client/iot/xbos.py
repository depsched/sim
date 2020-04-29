import time
import uuid
from . import k8s

from kubernetes import config
from kubernetes.client import Configuration
from kubernetes.client.apis import core_v1_api

from ..share_study import ecr
import random
from ..utils import cmd
from ..cmd import replace_image, replace_origin, replace

pod_start_time_map = {}
pod_startup_time_map = {}

seed = 42


# initial setup for kube-client
def init():
    global ecr_client, api
    ecr_client = ecr.ECRImageDB()
    config.load_kube_config()
    c = Configuration()
    c.assert_hostname = False
    api = core_v1_api.CoreV1Api()
    random.seed(seed)


def clear_images_from_workers(names):
    wip = k8s.get_worker_external_ip_map()
    worker_ips = list()

    for n, ls in k8s.get_node_map(selectors=k8s.default_excl_selectors, reverse=True).items():
        if "node-role.kubernetes.io/master" not in ls:
            worker_ips.append(wip[n])

    for w in worker_ips:
        for n in names:
            cmd("ssh -o 'StrictHostKeyChecking no' admin@%s 'sudo docker rmi %s'" % (w, n))


def run():
    # original
    # warm_cache()
    name = "xbos"
    image = "238764668013.dkr.ecr.us-west-2.amazonaws.com/xbos-microsvc_indoor_temperature_prediction"

    def exp(exp_name):
        remove(name=name)
        pull_times, sched_times, total_times = list(), list(), list()
        for _ in range(20):
            launch(name=name,
                   image=image,
                   duration=1)

            pull_time = get_pod_startup_time(name, blocking=True)
            pull_times.append(pull_time)
            # sched_times.append(sched_time)
            # total_times.append(pull_time + sched_time)
            remove(name=name)
            time.sleep(10)
        print(pull_times)
        dump_latencies(pull_times, "sys_eval_cdf_{}.csv".format(exp_name))

    # clear_images_from_workers([image])
    # replace_origin()
    # exp("agnostic")
    # time.sleep(30)

    # image
    # warm_cache()
    clear_images_from_workers([image])
    replace_image()
    exp("image")
    time.sleep(30)

    # layer
    clear_images_from_workers([image])
    replace()
    exp("layer")
    time.sleep(30)
    cmd("python3 -m build.kube.cluster down")


def get_pod_startup_time(name, blocking=True):
    pull_time = -1
    sched_time = -1

    is_term = False
    while not is_term:
        for pod in api.list_namespaced_pod(namespace="default").items:
            pod_name = pod.metadata.name
            if pod_name != name:
                continue

            sched_time = -1
            ready_time = -1

            if pod.status.conditions is None:
                continue

            for condition in pod.status.conditions:
                cond_type = condition.type
                if cond_type is None:
                    continue
                last_transition_time = condition.last_transition_time
                if cond_type == "Ready":
                    ready_time = last_transition_time.timestamp()
                if cond_type == "PodScheduled":
                    sched_time = last_transition_time.timestamp()
            time.sleep(3)
            container_state = pod.status.container_statuses[0].state.terminated
            if container_state:
                is_term = True
                container_start_time = container_state.started_at.timestamp()
                pull_time = container_start_time - sched_time
            # print(pod_name, ready_time - sched_time)
            if not ready_time == -1 and not sched_time == -1:
                sched_time = ready_time - sched_time
            time.sleep(0.2)

    print("average pull time:", pull_time)

    return pull_time
    # dump_latencies(latencies, "sys_eval_cdf_{}.csv".format(name))
    # dump_latencies(latencies, "sys_eval_cdf_opt_{}.csv".format(name))


def dump_latencies(result, result_file):
    # result = sorted(result)
    result_file = result_file + "-{}".format(str(time.time()))
    # counter = 1
    # base = len(result)
    with open(result_file, "w") as f:
        for r in result:
            # f.write("{},{}\n".format(r, counter / base))
            f.write("{}\n".format(r))
            # counter += 1


def launch(name, image, duration, scheduler="default-scheduler"):
    resp = None

    if not resp:
        print("Pod %s does not exits. Creating it..." % name)
        # check whether the name has tag if not, at the latest tag to it
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
                "activeDeadlineSeconds": 320,
                "restartPolicy": "Never",
                'containers': [{
                    'image': image,
                    'name': 'sleep',
                    "command": ["/bin/sh"],
                    "args": [
                        "-c",
                        "sleep {}".format(duration)
                    ]
                }]
            }
        }
        resp = api.create_namespaced_pod(body=pod_manifest,
                                         namespace='default',
                                         )
        print("Done.")
        return time.time()


def remove(name):
    cmd("kubectl delete pod " + name)


if __name__ == "__main__":
    init()
    run()
