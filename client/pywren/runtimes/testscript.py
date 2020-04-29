from __future__ import print_function
"""
Test script to run inside of runtime. Uses whatever version of pywren is installed
"""
import pywren 
import runtimes
import sys
import numpy as np
import importlib

OKGREEN = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'

package_failure = False
for runtime_name, runtime_config in runtimes.RUNTIMES.items():
    python_versions = runtime_config['pythonvers']
    for python_ver in python_versions:
        python_ver_major, python_ver_minor = python_ver.split(".")
        python_ver_major = int(python_ver_major)
        python_ver_minor = int(python_ver_minor)
        packages = runtime_config['packages']
        # skip all of the ones not for this version of python. Someday we should
        # be smarter about this
        if sys.version_info.major == python_ver_major \
           and sys.version_info.minor == python_ver_minor:
            print("running runtime config {}".format(runtime_name))

            # create an executor
            config = pywren.wrenconfig.default()
            staged_runtime_url, staged_meta_url = runtimes.get_staged_runtime_url(runtime_name, python_ver)
            assert staged_runtime_url[:5] == "s3://"
            splits = staged_runtime_url[5:].split("/")
            bucket = splits[0]
            key = "/".join(splits[1:])
            config['runtime']['bucket'] = bucket
            config['runtime']['s3_key'] = key
            print("running with bucket={} key={}".format(bucket, key))
            wrenexec = pywren.lambda_executor(config)

            def import_check(x):
                results = {}

                conda_results = {}
                for pkg in packages['conda_install']:
                    if pkg in runtimes.CONDA_TEST_STRS:
                        test_str = runtimes.CONDA_TEST_STRS[pkg]
                        try:
                            eval(test_str)
                            conda_results[pkg] = True
                        except ImportError:
                            conda_results[pkg] = False

                results['conda'] = conda_results


                pip_results = {}
                for pkg in packages['pip_install'] + packages['pip_upgrade']:
                    if pkg in runtimes.PIP_TEST_STRS:
                        test_str = runtimes.PIP_TEST_STRS[pkg]
                        try:
                            eval(test_str)
                            pip_results[pkg] = True
                        except ImportError:
                            pip_results[pkg] = False

                results['pip'] = pip_results

                return results

            fut = wrenexec.call_async(import_check, 2)
            result = fut.result()
            for check_set, check_vals in result.items():
                for package, success in check_vals.items():
                    if success:
                        success_str = OKGREEN + "SUCCESS" + ENDC
                    else:
                        success_str = FAIL + "FAIL" + ENDC
                        package_failure = True
                    print("{} : {} {} [{}]".format(runtime_name, check_set, package, success_str))


        else:
            print("skipping runtime config {}".format(runtime_name))

if package_failure:
    sys.exit(1)
