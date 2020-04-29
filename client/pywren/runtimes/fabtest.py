from fabric.api import local, env, run, put, cd, task, sudo, settings, warn_only, lcd, path, get
from fabric.contrib import project
import boto3
import cloudpickle
import json
import base64
import cPickle as pickle

CONDA_BUILD_DIR = "/tmp/conda"
CONDA_INSTALL_DIR = os.path.join(CONDA_BUILD_DIR, "condaruntime")

CONDA_DEFAULT_LIST = ["tblib", "numpy", "pytest", "Click", "numba", "boto3", "PyYAML", "cython", "boto", "scipy", "pillow", "cvxopt", "scikit-learn"]

PIP_DEFAULT_LIST = ['cvxpy', 'redis', 'glob2']
PIP_DEFAULT_UPGRADE_LIST = ['cloudpickle', 'enum34']

def create_runtime(pythonver, 
                   conda_packages, pip_packages, 
                   pip_upgrade_packages):
    

    conda_pkg_str = " ".join(conda_packages)
    pip_pkg_str = " ".join(pip_packages)
    pip_pkg_upgrade_str = " ".join(pip_upgrade_packages)
    run("rm -Rf {}".format(CONDA_BUILD_DIR))
    run("mkdir -p {}".format(CONDA_BUILD_DIR))
    with cd(CONDA_BUILD_DIR):
        run("wget https://repo.continuum.io/miniconda/Miniconda{}-latest-Linux-x86_64.sh -O miniconda.sh ".format(pythonver))
        
        run("bash miniconda.sh -b -p {}".format(CONDA_INSTALL_DIR)
        with path("{}/bin".format(CONDA_INSTALL_DIR), behavior="prepend"):

            run("conda install -q -y {}".format(conda_pkg_str))
            run("pip install {}".format(pip_pkg_str))
            run("pip install --upgrade {}".format(pip_pkg_upgrade_str))

RUNTIMES = {'keyname' : (3, CONDA_DEFAULT_LIST, 
                         PIP_DEFAULT_LIST, 
                         PIP_DEFAULT_UPGRADE_LIST)}

def build_runtimes():

    for k, v in RUNTIMES.items():
        execute(create_runtime, v[0], v[1], v[2], v[3])
        execute(shrink_conda, CONDA_INSTALL_DIR)

    
