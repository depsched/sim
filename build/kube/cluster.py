import os
import time
import uuid
from build.kube.utils import cmd, change_dir, cmd_check_success, timed
import build.kube.configs as cg

from build.kube.ig_gen.cmd import gen_block

from . import s3

_dir_path = os.path.dirname(os.path.realpath(__file__))
_script_dir = _dir_path + "/scripts/"
cmd = cmd

"""cluster ops"""


def new_spec(region="us-west-2", name=None, create_bucket=True, ins_groups=None):
    name = str(uuid.uuid4())[:5] if name is None else name

    # cluster spec
    with open(_script_dir + "/template.k8s.local.yaml", "r") as f:
        cluster_spec = f.read().replace("{{name}}", name).replace("{{region}}", region)

        if ins_groups is None:
            cluster_spec += "\n---\n\n"
            cluster_spec += gen_block("c4.xlarge", 1, region, name)
        else:
            for ins_type, num in ins_groups.items():
                cluster_spec += "\n---\n\n"
                cluster_spec += gen_block(ins_type, num, region, name)

    with open(_script_dir + "/spec.k8s.local.yaml", "w") as f:
        f.write(cluster_spec)

    # access node env variables
    with open(_script_dir + "/template-env.exp", "r") as f:
        cluster_env = f.read().replace("{{name}}", name).replace("{{region}}", region)

    # TODO: implement the delete cluster operation (separate the spec file..)

    with open(_script_dir + "/env.exp", "w") as f:
        f.write(cluster_env)

    # one bucket per cluster
    state_bucket = get_state_bucket()

    if create_bucket and not s3.bucket_exist(state_bucket):
        print("try creating bucket %s.." % state_bucket)
        s3.create_bucket(state_bucket, region)


def get_state_bucket():
    with open(_script_dir + "/env.exp", "r") as f:
        return f.readline().split("=")[1].rstrip() + "-state-store"


def prepare_cluster():
    cmd_with_bash(_script_dir + "s3-binary-store.sh")
    cmd_with_bash(_script_dir + "s3-state-store.sh")
    cmd_with_bash(_script_dir + "kops-iam.sh")


def create_cluster(ins_groups: dict = None, name=None):
    new_spec(name=os.environ.get("CLUSTER_NAME", name), region=os.environ.get("REGION", "us-west-2"), ins_groups=ins_groups)
    cmd_with_bash(_script_dir + "cluster.sh")


def start_cluster():
    cmd_with_bash(_script_dir + "start.sh")


def start_monitor():
    prometheus_path = _dir_path + "/prometheus"
    cmd("kubectl create namespace monitoring; kubectl apply -f {}".format(prometheus_path))


def start_prometheus_operator():
    cmd("kubectl apply -f https://raw.githubusercontent.com/kubernetes"
        "/kops/master/addons/prometheus-operator/v0.19.0.yaml --validate=false")
    cmd("kubectl get pods -n monitoring")


def start_metric_server():
    metric_server_path = _dir_path + "/../metrics-server"
    cmd("kubectl create -f {}/deploy/1.8+/".format(metric_server_path))


def stop_monitor():
    prometheus_path = _dir_path + "/../../thirdparty/prometheus"
    cmd("cd {}/contrib/kube-prometheus/; ./hack/cluster-monitoring/teardown".format(prometheus_path))


def port():
    # TODO: better way of getting the prometheus pod
    cmd("pkill kubectl || true")
    cmd("kubectl port-forward -n monitoring "
        "$(kubectl get pods -n monitoring -o name | grep prometheus-deployment | cut -d \"/\" -f2) "
        "{}:{} &>/dev/null &"
        .format(cg.prometheus_server_port, cg.prometheus_server_port))
    # TODO: replace the keep-alive with kubelet configurations to hold the tunnel
    cmd("watch -n120 curl localhost:{} &> /dev/null &".format(cg.prometheus_server_port))
    print("port forwarding completed.")


def config_ssh():
    """Replace the public key on the kube nodes with the launching machine's."""
    cmd_with_bash(_script_dir + "ssh-cred.sh")


def edit_cluster():
    cmd("source {}; kops edit ig nodes; kops update cluster $NAME --yes; kops rolling-update cluster --yes".format(
        _script_dir + "env.exp"))
    # print("-> use kops to further edit the default setup.")
    # print("-> most of the cluster setups are stored in the "
    #       "s3 state store.")


def export_cluster():
    cmd(_script_dir + "export.sh")


@change_dir
def delete_cluster():
    cmd_with_bash(_script_dir + "delete.sh")
    s3.delete_bucket(get_state_bucket())


def start_dashboard():
    print("starting kube-dashboard.")
    cmd("kubectl create -f {}sa-admin-user.yaml".format(_script_dir))
    cmd("kubectl create -f {}sa-clusterrolebinding-admin-user.yaml".format(_script_dir))
    cmd("kubectl create -f https://raw.githubusercontent.com/"
        "kubernetes/dashboard/master/src/deploy/recommended/kubernetes-dashboard.yaml")
    import time
    time.sleep(5)
    cmd("kubectl proxy >> ~/log/kube-build.log 2>&1 &")
    cmd("kubectl -n kube-system describe secret "
        "$(kubectl -n kube-system get secret | grep admin-user | awk '{print $1}')")
    print("done.")


def get_dashboard_token():
    cmd("kubectl -n kube-system describe secret "
        "$(kubectl -n kube-system get secret | grep admin-user | awk '{print $1}')")


@change_dir
def create_and_start(ins_groups: dict = None, cluster_name=None):
    create_cluster(ins_groups, cluster_name)
    print(ins_groups)
    start_cluster()
    # default addons
    # start_dashboard()
    # start_metric_server()
    # start_monitor()
    # start_prometheus_operator()
    # validate_svc()
    # setup the port forwarding
    print("waiting for the prom server to be ready..")
    time.sleep(20)
    # spark service role
    # spark_role()
    # install helm and tiller
    # install_helm()
    # port()


def is_up():
    return cmd_check_success("kubectl get nodes")


@timed
def restart_cluster():
    if cmd_check_success("kubectl get nodes"):
        delete_cluster()
    create_and_start()


def validate_svc():
    cmd("kubectl get svc -n kube-system")


"""kube ops"""


def upload_kube_binary():
    cmd_with_bash(_script_dir + "upload-binary.sh")


def make():
    cmd("cd {}; source env.exp; cd ${{KUBE_ROOT}}; "
        "make quick-release".format(_script_dir))


def make_and_upload():
    make()
    upload_kube_binary()


def api_change():
    cmd_with_bash(_script_dir + "api-change.sh")


"""others"""


def install_istio():
    cmd_with_bash(_script_dir + "istio.sh")


def install_schedulers():
    cmd("kubectl create -f {}".format(_dir_path + "/scheduler/origin-scheduler.yaml"))
    cmd("kubectl create -f {}".format(_dir_path + "/scheduler/image-scheduler.yaml"))


def install_fission():
    cmd_with_bash(_script_dir + "helm-server.sh")
    cmd_with_bash(_script_dir + "helm-client.sh")
    cmd_with_bash(_script_dir + "start.sh")


def install_helm():
    cmd_with_bash(_script_dir + "helm.sh")
    # cmd_with_bash(_script_dir + "helm-client.sh")


def cmd_with_bash(command):
    cmd("bash {}".format(command))


def main():
    from .utils import main_with_cmds
    import sys

    import os
    os.chdir(_script_dir)
    cmds = {
        # cluster operations
        "up": lambda: create_and_start({"c4.xlarge": 5}),
        "gen": lambda: new_spec(name="foo"),
        "prepare": prepare_cluster,
        "create": create_cluster,
        "start": start_cluster,
        "restart": restart_cluster,
        "edit": edit_cluster,
        "ssh": config_ssh,
        "down": delete_cluster,
        "export": export_cluster,
        # monitor
        "mon": start_metric_server,
        "start-mon": start_monitor,
        "stop-mon": stop_monitor,
        "config-ssh": lambda: cmd_with_bash(_script_dir + "ssh-cred.sh"),
        # kube-build operations
        "kube-all": make_and_upload,
        "api-change": api_change,
        "upload": upload_kube_binary,
        # install addons
        "ui": start_dashboard,
        "ui-token": get_dashboard_token,
        "ist": install_istio,
        "scheduler": install_schedulers,
        "fission": install_fission,
        # port forwardings
        "port": port,
        # helm
        "helm": install_helm,
    }

    main_with_cmds(cmds)


if __name__ == "__main__":
    main()
