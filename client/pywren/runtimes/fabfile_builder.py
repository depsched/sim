"""
fab -f fabfile_builer.py -R builder conda_setup_mkl conda_clean package_all


"""
from fabric.api import local, env, run, put, cd, task, sudo, settings, warn_only, lcd, path, get, execute
from fabric.contrib import project
import boto3
import cloudpickle
import json
import base64
import cPickle as pickle
import json
import runtimes
import yaml
import os

tgt_ami = 'ami-7172b611'
region = 'us-west-2'
unique_instance_name = 'pywren_builder'

s3url = "s3://ericmjonas-public/condaruntime.python3.stripped.scipy-cvxpy-sklearn.mkl_avx2.tar.gz"



def tags_to_dict(d):
    return {a['Key'] : a['Value'] for a in d}

def get_target_instance():
    res = []
    ec2 = boto3.resource('ec2', region_name=region)

    for i in ec2.instances.all():
        if i.state['Name'] == 'running':
            d = tags_to_dict(i.tags)
            if d['Name'] == unique_instance_name:
                res.append('ec2-user@{}'.format(i.public_dns_name))
    print "found", res
    return {'builder' : res}

env.roledefs.update(get_target_instance())

@task
def launch():

    ec2 = boto3.resource('ec2', region_name=region)

    instances = ec2.create_instances(ImageId=tgt_ami, MinCount=1, MaxCount=1, 
                         KeyName='ec2-us-west-2', InstanceType='m4.large')
    inst = instances[0]

    inst.wait_until_running()
    inst.reload()
    inst.create_tags(
        Resources=[
            inst.instance_id
        ],
        Tags=[
            {
                'Key': 'Name',
                'Value': unique_instance_name
            },
        ]
    )

@task        
def ssh():
    local("ssh -A " + env.host_string)

@task 
def install_gist():
    """
    https://github.com/yuichiroTCY/lear-gist-python
    """
    with cd("/tmp"):
        sudo("yum install -q -y  git gcc g++ make")
        run("rm -Rf gist")
        run("mkdir gist")
        with cd("gist"):
            run("git clone https://github.com/yuichiroTCY/lear-gist-python")
            with cd("lear-gist-python"):
                run("/tmp/conda/condaruntime/bin/conda install -q -y -c menpo fftw=3.3.4")
                run("sh download-lear.sh")
                run("sed -i '1s/^/#define M_PI 3.1415926535897\\n /' lear_gist-1.2/gist.c")
                run("CFLAGS=-std=c99 /tmp/conda/condaruntime/bin/python setup.py build_ext -I /tmp/conda/condaruntime/include/ -L /tmp/conda/condaruntime/lib/")
                run("CFLAGS=-std=c99 /tmp/conda/condaruntime/bin/python setup.py install")
            
@task
def shrink_conda(CONDA_RUNTIME_DIR):
    put("shrinkconda.py")
    run("python shrinkconda.py {}".format(CONDA_RUNTIME_DIR))

@task
def terminate():
    ec2 = boto3.resource('ec2', region_name=AWS_REGION)

    insts = []
    for i in ec2.instances.all():
        if i.state['Name'] == 'running':
            d = tags_to_dict(i.tags)
            if d['Name'] == instance_name:
                i.terminate()
                insts.append(i)



@task
def deploy():
        local('git ls-tree --full-tree --name-only -r HEAD > .git-files-list')
    
        project.rsync_project("/tmp/pywren", local_dir="./",
                              exclude=['*.npy', "*.ipynb", 'data', "*.mp4", 
                                       "*.pdf", "*.png"],
                              extra_opts='--files-from=.git-files-list')
        

CONDA_BUILD_DIR = "/tmp/conda"
CONDA_INSTALL_DIR = "/tmp/condaruntime"


def create_runtime(pythonver, 
                   conda_packages, pip_packages, 
                   pip_upgrade_packages):
    
    conda_pkgs_default_channel = []
    conda_pkgs_custom_channel = []
    for c in conda_packages:
        if isinstance(c, tuple):
            conda_pkgs_custom_channel.append(c)
        else:
            conda_pkgs_default_channel.append(c)
            
    conda_default_pkg_str = " ".join(conda_pkgs_default_channel)
    pip_pkg_str = " ".join(pip_packages)
    pip_pkg_upgrade_str = " ".join(pip_upgrade_packages)
    python_base_ver = pythonver.split(".")[0]
    run("rm -Rf {}".format(CONDA_BUILD_DIR))
    run("rm -Rf {}".format(CONDA_INSTALL_DIR))
    run("mkdir -p {}".format(CONDA_BUILD_DIR))
    with cd(CONDA_BUILD_DIR):
        run("wget https://repo.continuum.io/miniconda/Miniconda{}-latest-Linux-x86_64.sh -O miniconda.sh ".format(python_base_ver))
        
        run("bash miniconda.sh -b -p {}".format(CONDA_INSTALL_DIR))
        with path("{}/bin".format(CONDA_INSTALL_DIR), behavior="prepend"):
            run("conda install -q -y python={}".format(pythonver))
            run("conda install -q -y {}".format(conda_default_pkg_str))
            for chan, pkg in conda_pkgs_custom_channel:
                run("conda install -q -y -c {} {}".format(chan, pkg))
            run("pip install {}".format(pip_pkg_str))
            run("pip install --upgrade {}".format(pip_pkg_upgrade_str))

def format_freeze_str(x):
    packages = x.splitlines()
    return [a.split("==") for a in packages]

@task
def package_all(s3url):
    with cd(CONDA_INSTALL_DIR + "/../"):
        run("tar czf {} condaruntime".format(os.path.join(CONDA_BUILD_DIR, 'condaruntime.tar.gz')))
            
    with cd(CONDA_BUILD_DIR):
        get("condaruntime.tar.gz", local_path="/tmp/condaruntime.tar.gz")
        local("aws s3 cp /tmp/condaruntime.tar.gz {}".format(s3url))


def build_and_stage_runtime(runtime_name, runtime_config):
        python_ver = runtime_config['pythonver']
        conda_install = runtime_config['conda_install']
        pip_install = runtime_config['pip_install']
        pip_upgrade = runtime_config['pip_upgrade']
        execute(create_runtime, python_ver, conda_install,
                pip_install, pip_upgrade)
        execute(shrink_conda, CONDA_INSTALL_DIR)
        freeze_str = execute(get_runtime_pip_freeze, CONDA_INSTALL_DIR)
        freeze_str_single = freeze_str.values()[0] # HACK 

        freeze_pkgs = format_freeze_str(freeze_str_single)

        preinstalls_str = execute(get_preinstalls, CONDA_INSTALL_DIR)
        preinstalls_str_single = preinstalls_str.values()[0]
        preinstalls = json.loads(preinstalls_str_single)
        conda_env_yaml = execute(get_conda_root_env, CONDA_INSTALL_DIR)
        conda_env_yaml_single = conda_env_yaml.values()[0]  # HACK
        pickle.dump(conda_env_yaml_single, open("debug.pickle", 'w'))
        conda_env = yaml.load(conda_env_yaml_single)
        runtime_dict = {'python_ver' : python_ver, 
                        'conda_install' : conda_install,
                        'pip_install' : pip_install, 
                        'pip_upgrade' : pip_upgrade,
                        'pkg_ver_list' : freeze_pkgs,
                        'preinstalls' : preinstalls, 
                        'conda_env_config': conda_env}
        
        # Use a single url for staging
        runtime_tar_gz, runtime_meta_json = \
            runtimes.get_staged_runtime_url(runtime_name, python_ver)

        urls = [runtime_tar_gz]
        runtime_dict['urls'] = urls

        execute(package_all, runtime_tar_gz)
        with open('runtime.meta.json', 'w') as outfile:
            json.dump(runtime_dict, outfile)        
            outfile.flush()

        local("aws s3 cp runtime.meta.json {}".format(runtime_meta_json))

@task
def build_all_runtimes():
    for runtime_name, rc in runtimes.RUNTIMES.items():
        for pythonver in rc['pythonvers']:
            rc2 = rc['packages'].copy()
            rc2['pythonver'] = pythonver
            build_and_stage_runtime(runtime_name, rc2)

@task 
def build_single_runtime(runtime_name, pythonver):
    rc = runtimes.RUNTIMES[runtime_name]
    rc2 = rc['packages'].copy()
    rc2['pythonver'] = pythonver
    build_and_stage_runtime(runtime_name, rc2)

@task
def get_runtime_pip_freeze(conda_install_dir):
    return run("{}/bin/pip freeze 2>/dev/null".format(conda_install_dir))

@task
def get_preinstalls(conda_install_dir):
    return run('{}/bin/python -c "import pkgutil;import json;print(json.dumps([(mod, is_pkg) for _, mod, is_pkg in pkgutil.iter_modules()]))"'.format(conda_install_dir))


@task
def get_conda_root_env(conda_install_dir):
    return run("{}/bin/conda env export -n root 2>/dev/null".format(conda_install_dir))

def deploy_runtime(runtime_name, python_ver):
    # move from staging to production
    staging_runtime_tar_gz, staging_runtime_meta_json \
        = runtimes.get_staged_runtime_url(runtime_name, python_ver)

    runtime_tar_gz, runtime_meta_json = runtimes.get_runtime_url(runtime_name, 
                                                                 python_ver)

    local("aws s3 cp {} {}".format(staging_runtime_tar_gz, 
                                   runtime_tar_gz))

    local("aws s3 cp {} {}".format(staging_runtime_meta_json, 
                                   runtime_meta_json))


@task 
def deploy_runtimes(num_shards=10):
    num_shards = int(num_shards)
    for runtime_name, rc in runtimes.RUNTIMES.items():
        for python_ver in rc['pythonvers']:
            staging_runtime_tar_gz, staging_runtime_meta_json \
                = runtimes.get_staged_runtime_url(runtime_name, python_ver)

            # Always upload to the base tar gz url.
            base_tar_gz = runtimes.get_runtime_url_from_staging(staging_runtime_tar_gz)
            local("aws s3 cp {} {}".format(staging_runtime_tar_gz,
                                           base_tar_gz))

            runtime_meta_json_url = runtimes.get_runtime_url_from_staging(staging_runtime_meta_json)
            # If required, generate the shard urls and update metadata
            if num_shards > 1:
                local("aws s3 cp {} runtime.meta.json".format(staging_runtime_meta_json))
                meta_dict = json.load(open('runtime.meta.json', 'r'))
                shard_urls = []
                for shard_id in xrange(num_shards):
                    bucket_name, key = runtimes.split_s3_url(base_tar_gz)
                    shard_key = runtimes.get_s3_shard(key, shard_id)
                    hash_s3_key = runtimes.hash_s3_key(shard_key)
                    shard_url = "s3://{}/{}".format(bucket_name, hash_s3_key)
                    local("aws s3 cp {} {}".format(base_tar_gz, 
                                                   shard_url))
                    shard_urls.append(shard_url)

                meta_dict['urls'] = shard_urls
                with open('runtime.meta.json', 'w') as outfile:
                    json.dump(meta_dict, outfile)
                    outfile.flush()
                local("aws s3 cp runtime.meta.json {}".format(runtime_meta_json_url))
            else:
                local("aws s3 cp {} {}".format(staging_runtime_meta_json,
                                               runtime_meta_json_url))
