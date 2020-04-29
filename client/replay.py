import time
import uuid

from kubernetes import config
from kubernetes.client import Configuration
from kubernetes.client.apis import core_v1_api
from kubernetes.client.rest import ApiException

from .share_study import ecr
from .utils import official_images, ecr_official_images
import random
import cloudpickle as pickle
import redis
import numpy as np
from .utils import cmd
from .cmd import replace_image, replace_origin, replace

pod_start_time_map = {}
pod_startup_time_map = {}

black_set = {'storm-uix', 'bhuisgen-docker-zabbix-coreosx', 'ubuntu-16x', 'kent72-lanternx',
             'alino-data-only-containerx', 'meajudaufbax', 'consul-agentx', 'eeacms-memcachedx', 'slc6-wqx', 'spigotx',
             'kafka-managerx', 'catalysts-firefoxx', 'consul-serverx', 'logitech-media-serverx', 'standalone-chromex',
             'shairport-syncx', 'plexx', 'instructure-fake-s3x', 'adito-icinga2x', 'rtorrent-rutorrentx',
             'webdevops-phpx', '1and1internet-ubuntu-16-nginx-passenger-python-2x', 'container-exporterx',
             'diamond-containercollectorx', 'maschmid-phantomjsx', 'muallin-yacreaderlibrary-server-dockerx',
             '1and1internet-ubuntu-16-nginx-passengerx', 'rix1337-docker-organizrx', 'frodenas-logstashx',
             'backup-all-mysqlx', 'clex', 'ubuntu-16-nginx-php-5.6-wordpress-4x', 'standalone-chrome-debugx',
             'jkbsz-steam-tf2x', 'prometheus-rancher-metadata-confx', 'siegex', 'icinga2x', 'barcus-bareos-directorx',
             'sinap-timing-epics-iocx', 'basi-grafanax', 'wso2dssx', 'appdaemonx', 'failerx',
             'digitalwonderland-zookeeperx', 'an0t8-mariadbx', 'komljen-logstashx', 'transmission-openvpnx',
             'shepherdx', 'mongodb-exporterx', 'mailhogx', 'kubeless-uix', 'cytopia-php-fpm-7.0x', 'frodenas-rabbitmqx',
             'imageproxyx', 'steamcache-dnsx', 'pritunlx', 'gitlab-cex', 'ponycx', 'standalone-firefoxx', 'serverx',
             'node-firefox-debugx', 'alpine-lampx', 'ardb-serverx', 'cytopia-php-fpm-5.4x', 'skydnsx', 'sonarrx',
             'standalone-firefox-debugx', 'newrelic-daemonx', 'pms-dockerx', 'chromex', 'confluent-zookeeperx',
             'stenote-docker-lempx', 'rutorrent-modx', 'docker-gocron-logrotatex',
             '1and1internet-ubuntu-16-nginx-passenger-nodejs-4x', 'ubuntu-madsonicx', 'appsvctest-rubyx',
             'logitechmediaserverx', 'iota-nodex', 'dkdnsx', 'squid-with-net-speederx', 'totem-yoda-proxyx', 'datax',
             'haocen-lanternx', 'hazelcastx', 'node-chrome-debugx', 'orcx', 'unblibraries-nginx-phpx',
             'guacamole-webserverx', 'spamdx', 'microbox-etcdx', 'digitalwonderland-influxdbx', 'grafana-xxlx',
             'daemonx', 'ubuntu-16-apache-php-7.1x', 'kcpssx', 'pipesx', 'ssr-with-net-speederx',
             'zabbix-web-nginx-pgsqlx', 'rtailx', 'kafka-manager-dockerx', 'piwigox', 'beehive-certx', 'mondain-red5x',
             'bobrik-kibana4x', 'currentweatherx', 'greggigon-apachedsx', 'metricbeatx', 'jpillora-dnsmasqx',
             'sonarr-centosx', 'rungeict-cloudflare-railgunx', 'nimmis-ubuntux', 'hambax', 'picoded-glusterfs-clientx',
             'frodenas-postgresqlx', 'exemplator-pagex', 'ubuntu-systemdx', 'phenompeople-neo4jx', 'egofelix-mariadbx',
             'docker-jenkins-slavex', 'openshift-nginxx', 'dwolla-javax', 'whereisaaron-grafana-kubernetesx',
             'nazarpc-phpmyadminx', 'docker-nginxx', 'docker-logstashx', 'teamspeakx', 'axibase-atsdx',
             'docisin-custom-apachex', 'appsvctest-nodex', 'shaarlix', 'configurable-http-proxyx',
             'docker-lloyd-localx', 'jamesyale-docker-ebot-csgox', 'travix-couchbase-exporterx', 'guide-serverx',
             'nginx-request-exporterx', 'dockerdashx', 'monero-minerx', 'pushgatewayx', 'gremlinx',
             'ubuntu-16-nginx-php-7.1-joomla-3x', 'docker-webuix', 'rconx', 'graphite-exporterx', 'ephemeral-npmx',
             'dockbeatx', 'sailfrog-cypht-dockerx', 'rust-musl-builderx', 'grrx', 'payara-microx', 'enieuw-jenkinsx',
             'elkarbackupx', 'docker-sickragex', 'sbobylev-docker-sample-flask-appx', 'frodenas-couchdbx',
             'dminkovsky-skydnsx', 'ubuntu-transmissionx', 'frodenas-elasticsearchx', 'kuryr-demox',
             'teamcity-agent-dotnet-corex', 'hubx', 'frontailx', 'golang-hellox', 'turbine-hystrix-dashboardx',
             'web-dvwax', 'justbuchanan-zetta-browser-dockerx', 'docker-zk-exhibitorx', 'skilldlabs-mailhogx', 'nutchx',
             'darksheer-ubuntux', 'vpn-pptpx', 'docker-dangling-httpx', 'docker-jenkins-slave-datahubx',
             'sunidhi-docker-postgresqlx', 'jenkins-slave-imagex', 'socatx', 'sstarcher-uchiwax', 'timhaak-plexx',
             'nodex', 'phenompeople-mongodbx', 'camptocamp-postgisx', 'basi-jenkinsx', 'ca-adpq-protox',
             'ubuntu-16-rspecx', 'uhttpdx', 'rizkyario-isara-knowledgex', 'docker-cloudwatch-monitoringx',
             'ubuntu-16-apache-php-5.6x', 'masterx', 'keepalivedx', 'twang2218-gitlab-ce-zhx', 'mprasil-dokuwikix',
             'xiocode-shadowsocks-libevx', 'opencpu-rstudiox', 'master-multichainx', 'coppit-dansguardianx',
             'camptocamp-puppetserverx', 'glancesx', 'docker-usergridx', 'paas-in-a-boxx', 'rqircx',
             'vulnerables-cve-2016-10033x', 'zendx', 'giantswarm-helloworldx', 'rocketchatx', 'docktorrentx',
             'cjongseok-consulx', 'danieldent-postgres-replicationx', 'dvdmuckle-curl-a-jokex', 'rundeckx',
             'terasologyx', 'opensslx', 'docker-mailslurperx', 'fluentd-kubernetesx', 'artifactoryx',
             'springboot-dockerx', 'buddho-dronex', 'node-firefoxx', 'screwdriverx', 'k8s-etcdx',
             'ubuntu-16-customersshx', 'openvasx', 's2i-gradle-javax', 'eventstorex', 'buildx', 'tozd-postfixx',
             'sdesbure-arch-jackettx', 'concoursex', 'authx', 'clickhouse-clientx', 'sinusbotx', 'alpine-apache-phpx',
             'portusx'}
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


def get_ecr_images():
    print("loading image list..")
    r = redis.StrictRedis(host='localhost')
    image_zipf_list = pickle.loads(r.get("image_pop_list_zipf"))
    return image_zipf_list


def gen_req_seq(length, load, duration):
    from .share_study.ecr import ECRImageDB

    # parameters
    load = load
    duration = duration

    # expt length
    length = length

    ecr_db = ECRImageDB()
    image_zipf_list = sorted(get_ecr_images(), key=lambda x: x[1], reverse=True)[:2000]
    sampler = weighted_sampler(image_zipf_list)
    print(len(image_zipf_list))

    req_seq = [[(sampler(), random.randint(1, duration)) for _ in range(np.random.poisson(load))] for _ in
               range(length)]

    return req_seq


def run():
    cmd("python3 -m build.kube.cluster up >/dev/null &")
    time.sleep(400)
    init()

    for load, duration in [(10, 1), (1, 3)]:
        req_seq = gen_req_seq(length=500, load=load, duration=duration)
        # original
        # warm_cache()
        replace_origin()
        replay_sim(req_seq=req_seq, tag="agnostic_{}_{}".format(load, duration))
        # restart_cluster()

        # image
        # warm_cache()
        # replace_image()
        # replay_sim(req_seq=req_seq, tag="image_{}_{}".format(load, duration))
        # restart_cluster()

        # layer
        # warm_cache()
        # replace()
        # replay_sim(req_seq=req_seq, tag="layer_{}_{}".format(load, duration))
    # cmd("python3 -m build.kube.cluster down")


def replay_sim(req_seq=[], tag="", output=True):
    import random
    print("assuming the cluster is up and running..")
    init()
    req_seq = gen_req_seq(500, 10, 1)

    # setup for pods
    prefix = "238764668013.dkr.ecr.us-west-1.amazonaws.com/"
    name_template = "{}"

    # submit pod requests
    for i, images in enumerate(req_seq):
        for image, duration in images:
            pod_name = name_template.format(image) + "--" + str(uuid.uuid4()).replace("_", "-")
            pod_name = pod_name.replace("_", "-")
            pod_name = pod_name.replace("/", "-")
            print(image, "{}/{}".format(i, len(req_seq)))
            image = image.replace("/", "-")
            image = prefix + image
            start_time = launch(pod_name, image, duration)
            pod_start_time_map[pod_name] = start_time
        time.sleep(1)
    # wait till all finishes
    time.sleep(60)
    if output:
        get_pod_startup_time(tag)


def get_pod_startup_time(tag=""):
    pod_list = api.list_namespaced_pod(namespace="default")
    latencies = []
    latencies_opt = []
    for pod in pod_list.items:
        pod_name = pod.metadata.name
        sched_time = -1
        ready_time = -1
        container_start_time = -1
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
        container_state = pod.status.container_statuses[0].state.terminated
        if container_state:
            container_start_time = container_state.started_at.timestamp()
            latencies_opt.append(container_start_time - sched_time)
        # print(pod_name, ready_time - sched_time)
        if not ready_time == -1 and not sched_time == -1:
            latencies.append(ready_time - sched_time)
    dump_latencies(latencies, "sys_eval_cdf_{}.csv".format(tag))
    dump_latencies(latencies, "sys_eval_cdf_opt_{}.csv".format(tag))
    print(np.mean(latencies), len(latencies))
    print(np.mean(latencies_opt), len(latencies_opt))


def dump_latencies(result, result_file):
    result = sorted(result)
    result_file = result_file + "-{}".format(str(time.time()))
    counter = 1
    base = len(result)
    with open(result_file, "w") as f:
        for r in result:
            f.write("{},{}\n".format(r, counter / base))
            counter += 1


def replay():
    # setup for pods
    prefix = "238764668013.dkr.ecr.us-west-1.amazonaws.com/"
    name_template = "{}"

    # submit pod requests
    images = get_ecr_images()[100:200]
    for i, image in enumerate(images):
        image = image[0]
        name = name_template.format(image) + str(uuid.uuid4())
        image = prefix + image
        # launch(name, image, random.randint(1, 10))
        launch(name, image, 1)
        time.sleep(1)


def restart_cluster():
    cmd("python3 -m build.kube.cluster down")
    cmd("python3 -m build.kube.cluster up >/dev/null &")
    time.sleep(500)


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


def nginx():
    launch("nginx-test" + str(uuid.uuid4()), "nginx", "1")


def check_official_images():
    image_map = {}
    for image in get_images():
        image_map[image] = True
    counter = len(official_images)
    images = []
    for image in official_images:
        if image not in image_map:
            print("missing image {} on ecr".format(image))
            counter -= 1
            continue
        images.append(image)
    print("{} official images on ecr".format(counter))
    for i in random.sample(images, 100):
        print(i)


def get_images():
    return ecr_client.get_ecr_repositories()


def weighted_sampler(source):
    from bisect import bisect
    """Return one sample from the source sequence by weighted sampling."""
    total = 0
    cum_weights = []

    def gen_cumweights(source):
        nonlocal total
        v, weights = zip(*source)
        for w in weights:
            total += w
            cum_weights.append(total)

    def sampler():
        x = random.random() * total
        i = bisect(cum_weights, x)
        return source[i][0]

    gen_cumweights(source)
    return sampler


if __name__ == "__main__":
    init()
    print(check_official_images())
