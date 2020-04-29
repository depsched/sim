import yaml
import os
from pprint import pprint

from kubernetes import client, config
from .utils import cmd

_dir_path = os.path.dirname(os.path.realpath(__file__))
_manifest_file = _dir_path + "/manifests/{}.yaml"


def init():
    global v1
    try:
        config.load_kube_config()
        v1 = client.CoreV1Api()
    except:
        pass


init()

deployment_name = "nginx-deployment"
# deployment_name = "misc-deployment"
deployment_file = _manifest_file.format(deployment_name)
namespace = "default"

master_ip = None


def list_pods():
    print("Listing pods with their IPs:")
    ret = v1.list_pod_for_all_namespaces(watch=False)
    for i in ret.items:
        print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))


def get_all_pods():
    return v1.list_pod_for_all_namespaces(watch=False)


def get_startup_time():
    pods = get_all_pods()
    times = []
    for pod in pods.items():
        print(pod)
        return


def check_ready():
    ret = v1.list_namespaced_pod(namespace)
    pod_name = ret.items[0].metadata.name
    ret = v1.read_namespaced_pod_status(pod_name, namespace, pretty=True)
    print(ret)


def create_deployment():
    with open(deployment_file, "r") as f:
        dep = yaml.load(f)
        k8s_beta = client.ExtensionsV1beta1Api()
        resp = k8s_beta.create_namespaced_deployment(
            body=dep, namespace=namespace)
        print("Deployment created. status='%s'" % str(resp.status))


def delete_deployment():
    v1 = client.AppsV1Api()
    # body = client.V1DeleteOptions()
    pprint(v1.list_deployment_for_all_namespaces())
    pprint(v1.get_api_resources())
    # ret = v1.delete_namespaced_deployment(name=deployment_name, namespace=namespace, body=body)
    # pprint(ret)


def delete_all_pods():
    cmd("kubectl delete pods --all")


def get_master_ip():
    init()
    ret = v1.list_node(label_selector="kubernetes.io/role=master")
    # return "ec2-13-57-224-89.us-west-1.compute.amazonaws.com"
    for i in ret.items:
        for entry in i.status.addresses:
            if entry.type == "ExternalIP":
                global master_ip
                master_ip = entry.address
                return entry.address


def get_nodes_ip():
    ret = v1.list_node(label_selector="kubernetes.io/role=node")
    addresses = []
    for i in ret.items:
        for entry in i.status.addresses:
            if entry.type == "ExternalIP":
                addresses.append(entry.address)
    return addresses


def clean_node_image(image):
    ips = get_nodes_ip()
    for ip in ips:
        cmd("ssh -o \"StrictHostKeyChecking no\" -t admin@{} \"sudo docker rmi -f {}\"".format(ip, image))


def show_node_image():
    ips = get_nodes_ip()
    for ip in ips:
        cmd("ssh -o \"StrictHostKeyChecking no\" -t admin@{} \"sudo docker images\"".format(ip))


def save():
    global master_ip
    if master_ip is None:
        master_ip = get_master_ip()
    cmd("ssh -o \"StrictHostKeyChecking no\" -t admin@{} \"echo hello\"".format(master_ip))
    cmd("scp admin@{}:/etc/kubernetes/manifests/kube-scheduler.manifest {}"
        .format(master_ip, _manifest_file.format("kube-scheduler")))


def replace_origin():
    global master_ip
    if master_ip is None:
        master_ip = get_master_ip()
    cmd("ssh -o \"StrictHostKeyChecking no\" -t admin@{} \"sudo chmod "
        "777 /etc/kubernetes/manifests/kube-scheduler.manifest\"".format(master_ip))
    cmd("scp {} admin@{}:/etc/kubernetes/manifests/kube-scheduler.manifest"
        .format(_manifest_file.format("origin-scheduler"), master_ip))
    # cmd("ssh -t admin@{} \"sudo systemctl restart kubelet\"".format(master_ip))


def replace_image():
    global master_ip
    if master_ip is None:
        master_ip = get_master_ip()
    cmd("ssh -o \"StrictHostKeyChecking no\" -t admin@{} \"sudo chmod "
        "777 /etc/kubernetes/manifests/kube-scheduler.manifest\"".format(master_ip))
    cmd("scp {} admin@{}:/etc/kubernetes/manifests/kube-scheduler.manifest"
        .format(_manifest_file.format("image-scheduler"), master_ip))


def replace_image_focused():
    global master_ip
    if master_ip is None:
        master_ip = get_master_ip()
    cmd("ssh -o \"StrictHostKeyChecking no\" -t admin@{} \"sudo chmod "
        "777 /etc/kubernetes/manifests/kube-scheduler.manifest\"".format(master_ip))
    cmd("scp {} admin@{}:/etc/kubernetes/manifests/kube-scheduler.manifest"
        .format(_manifest_file.format("image-focused-scheduler"), master_ip))


def replace_unfocused():
    global master_ip
    if master_ip is None:
        master_ip = get_master_ip()
    cmd("ssh -o \"StrictHostKeyChecking no\" -t admin@{} \"sudo chmod "
        "777 /etc/kubernetes/manifests/kube-scheduler.manifest\"".format(master_ip))
    cmd("scp {} admin@{}:/etc/kubernetes/manifests/kube-scheduler.manifest"
        .format(_manifest_file.format("kube-unfocused-scheduler"), master_ip))


def replace():
    global master_ip
    if master_ip is None:
        master_ip = get_master_ip()
    cmd("ssh -o \"StrictHostKeyChecking no\" -t admin@{} \"sudo chmod "
        "777 /etc/kubernetes/manifests/kube-scheduler.manifest\"".format(master_ip))
    cmd("scp {} admin@{}:/etc/kubernetes/manifests/kube-scheduler.manifest"
        .format(_manifest_file.format("kube-scheduler"), master_ip))


def replace_layer():
    global master_ip
    if master_ip is None:
        master_ip = get_master_ip()
    cmd("ssh -o \"StrictHostKeyChecking no\" -t admin@{} \"sudo chmod "
        "777 /etc/kubernetes/manifests/kube-scheduler.manifest\"".format(master_ip))
    cmd("scp {} admin@{}:/etc/kubernetes/manifests/kube-scheduler.manifest"
        .format(_manifest_file.format("layer-scheduler"), master_ip))


def get_scheduler_log():
    global master_ip
    if master_ip is None:
        master_ip = get_master_ip()
    cmd("ssh -o \"StrictHostKeyChecking no\" "
        "-t admin@{} \"sudo chmod 777 /var/log/kube-scheduler.log\"".format(master_ip))
    cmd("scp admin@{}:/var/log/kube-scheduler.log ./logs".format(master_ip))
    cmd("cat ./logs/kube-scheduler.log")
    #
    # cmd("ssh -o \"StrictHostKeyChecking no\" "
    #     "-t admin@{} \"sudo chmod 777 /var/log/kube-scheduler.log.1\"".format(master_ip))
    # cmd("scp admin@{}:/var/log/kube-scheduler.log.1 ./logs".format(master_ip))
    # cmd("cat ./logs/kube-scheduler.log.1")


def replay():
    from .replay import replay, init
    global master_ip
    init()
    if master_ip is None:
        master_ip = get_master_ip()
    cmd("ssh -t admin@{} \"sudo cp /dev/null /var/log/kube-scheduler.log\"".format(master_ip))
    replay()


def parse_log():
    import numpy as np
    log_file = "./logs/kube-scheduler.log"
    times = []
    with open(log_file, "r") as f:
        for line in f.readlines():
            line = line.split()
            tag = line[4]
            if tag == "$[sched]":
                times.append(int(line[5]))
    mean_lat = np.mean(times)
    print(mean_lat)
    return mean_lat


def validate():
    from .replay import replay_sim
    replay_sim()


def test():
    clean_node_image("238764668013.dkr.ecr.us-west-1.amazonaws.com/wren-default")
    show_node_image()


def pull():
    from .utils import official_images
    import numpy as np
    from .share_study import ecr
    import time
    latencies = {}
    # for image in official_images:
    #     cmd("docker pull {}".format(image))
    for image in official_images:
        start = time.time()
        cmd("docker run -d {}".format(image))
        latency = str(round(time.time() - start, 3))
        latencies[image] = latency
        cmd("docker stop $(docker ps -aq)")
        print(latency)
    print("\n".join(latencies))
    size_dict = {}
    with open("./plot-ext/size.txt") as f:
        lines = f.readlines()
        for line in lines:
            name = line.split()[0]
            size = line.split()[-1]
            size_dict[name] = size
    for name, latency in latencies.items():
        if name in size_dict:
            print(name, latency, size_dict[name])


def main():
    from .utils import main_with_cmds
    from .replay import nginx
    from .executor import warm
    from .replay import get_pod_startup_time, replay_sim, run

    cmds = {
        "deploy": create_deployment,
        "delete-dep": delete_deployment,
        "delete": delete_all_pods,
        "check": check_ready,
        "replace-origin": replace_origin,
        "replace-image": replace_image,
        "replace-image-focused": replace_image_focused,
        "replace-unfocused": replace_unfocused,
        "replace": replace,
        "validate": validate,
        "test": test,
        "save-manifest": save,
        "collect": get_startup_time,
        "list": list_pods,
        "log": get_scheduler_log,
        "nginx": nginx,
        "replay": replay,
        "parse": parse_log,
        "pull": pull,
        "show": show_node_image,
        "warm": warm,
        "time": get_pod_startup_time,
        "sim": replay_sim,
        "run": run,
    }
    main_with_cmds(cmds)


if __name__ == "__main__":
    main()
