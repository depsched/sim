import pprint as pp
from collections import defaultdict

from kubernetes import config
from kubernetes import client
from typing import Iterable

from . import cmd

default_excl_selectors = {
    "kubernetes.io/role": "master",
    "kops.k8s.io/instancegroup": "nodes.m4.2xlarge.prom",
}

_config_loaded = False


def create(spec=None):
    pass


def make_node_affinity_in_spec(selectors):
    return {
        "affinity": {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": k,
                                    "operator": "In",
                                    "values": [v],
                                } for k, v in selectors.items()
                            ],
                        }
                    ],
                },
            },
        },
    }


def load_kube_config(reload=False):
    global _config_loaded
    try:
        # per op cost is about 16ms, cache it
        if reload or not _config_loaded:
            print("k8s config reloaded..")
            config.load_kube_config()
            _config_loaded = True
    except Exception as e:
        print("running without k8s running or "
              "exposed in the context: {}.".format(str(e)))


def get_all_pods_name(ns: str = "default") -> Iterable[str]:
    load_kube_config()
    c = client.CoreV1Api()

    pods = c.list_namespaced_pod(namespace=ns).items
    return [pod.metadata.name for pod in pods]


def get_all_pods(ns: str = "default"):
    load_kube_config()
    c = client.CoreV1Api()

    pods = c.list_namespaced_pod(namespace=ns).items
    return pods


def delete_pod(name: str, ns: str = "default"):
    load_kube_config()
    c = client.CoreV1Api()

    body = client.V1DeleteOptions()
    c.delete_namespaced_pod(name, ns)


def delete_svc(name: str, ns: str = "default"):
    cmd("kubectl delete svc {} -n {}".format(name, ns))


def delete_deployment(name: str, ns: str = "default"):
    cmd("kubectl delete deploy {} -n ".format(name) + ns)


def delete_all_pods(ns: str = "default"):
    cmd("kubectl delete pods --all -n " + ns)


def delete_all_deploy(ns: str = "default"):
    cmd("kubectl delete deploy --all -n " + ns)


def delete_all_svc(ns: str = "default"):
    cmd("kubectl delete svc --all -n " + ns)


def delete_all_resource(ns: str = "default"):
    # note that the order matters
    delete_all_svc(ns)
    delete_all_deploy(ns)
    delete_all_pods(ns)


def get_node_pod_map(ns: str = "default", selectors=default_excl_selectors, reverse=True):
    """Pod by node name."""
    pods = get_all_pods(ns)
    nodes = get_all_nodes()

    node_pod_map = {
        n.metadata.labels["kubernetes.io/hostname"]: list()
        for n in nodes if selectors is None or selector_match(n.metadata.labels, selectors, reverse=reverse)
    }

    for p in pods:
        try:
            node_pod_map[p.spec.node_name].append(p)
        except:
            pass
    return node_pod_map


def get_node_pod_count(ns: str = "default", selectors=default_excl_selectors, reverse=True):
    npm = get_node_pod_map(ns, selectors, reverse=reverse)

    return {
        k: len(v) for k, v in npm.items()
    }


def get_all_nodes():
    load_kube_config()
    c = client.CoreV1Api()

    nodes = c.list_node()
    return [n for n in nodes.items]


def get_node_map(selectors=default_excl_selectors, reverse=True, full_object=False):
    """Hostname is used as the the key, the entire label set as the value."""
    nodes = get_all_nodes()
    node_map = dict()

    for n in nodes:
        labels = n.metadata.labels
        if selectors is None or selector_match(labels, selectors, reverse=reverse):
            name = labels["kubernetes.io/hostname"]
            node_map[name] = labels if not full_object else n
    return node_map


def get_worker_external_ip_map() -> dict:
    ips = dict()

    for n, info in get_node_map(full_object=True).items():
        for addr in info.status.addresses:
            if addr.type == "ExternalIP":
                ips[n] = addr.address
    return ips


def get_worker_external_internal_ip_map() -> dict:
    ext_in = dict()

    for n, info in get_node_map(full_object=True).items():
        k, v = None, None
        for addr in info.status.addresses:
            if addr.type == "ExternalIP":
                k = addr.address
            elif addr.type == "InternalIP":
                v = addr.address
        assert not (k is None or v is None)
        ext_in[k] = v
    return ext_in


def selector_match(labels, selectors, reverse=False):
    """If reverse is True, returns False for any matching. This makes exclusive matching easy."""

    for s, v in selectors.items():
        assert s in labels, "non-exist selector: " + s

        if (reverse and labels[s] == v) or (not reverse and labels[s] != v):
            return False
    return True


def get_node_labels(name) -> dict:
    return get_node_map()[name]
