import argparse
import os
import sys
import subprocess
import re
import boto3
import pprint as pp

ECR_REGISTRY_ADDRESS = None
AWS_REGION = "us-west-1"
ecr_client = None


def push_to_ecr(image_name):
    """Update the image repository, push, and write to the ECR image list."""
    if not ECR_REGISTRY_ADDRESS:
        login_ecr()
    ecr_repo = "{}/{}".format(ECR_REGISTRY_ADDRESS, image_name)
    cmd("docker tag {image_name} {ecr_repo}".format(image_name=image_name,
                                                    ecr_repo=ecr_repo))
    cmd("docker push {}".format(ecr_repo))


def login_ecr():
    """Login from local docker client to the ec2 repository."""
    global ECR_REGISTRY_ADDRESS, IS_LOGIN
    if "IS_LOGIN" in globals() and globals()["IS_LOGIN"]:
        return ECR_REGISTRY_ADDRESS

    def _ecr_credentials():
        out = cmd("aws ecr get-login --no-include-email --region {}".format(AWS_REGION), quiet=True)
        return out.decode("ascii").rstrip()

    ECR_CREDENTIALS = _ecr_credentials()
    ECR_REGISTRY_ADDRESS = re.search("(?<=https://).*", ECR_CREDENTIALS).group(0)
    cmd(ECR_CREDENTIALS);
    IS_LOGIN = True
    return ECR_REGISTRY_ADDRESS


def create_ecr_repository(name):
    global ecr_client
    if ecr_client is None:
        ecr_client = boto3.client('ecr')
    response = ""
    try:
        response = ecr_client.create_repository(
            repositoryName=name,
        )
        print("--> created repository: {}.".format(name))
        pp.pprint(response)
    except Exception as e:
        pp.pprint(response)
        print("--> unable to create repository: {}"
              "; move on.".format(name))
        print(e)


def main_with_cmds(cmds={}, add_arguments=None):
    def _print_usage(parser):
        parser.print_help(file=sys.stderr)
        sys.exit(2)

    parser = argparse.ArgumentParser(description='Collector cmds.')

    cmds.update({
        'argtest': lambda: print("halo, arg arg."),
        'help': lambda: _print_usage(parser),
    })

    for name in list(cmds.keys()):
        if '_' in name:
            cmds[name.replace('_', '-')] = cmds[name]

    cmdlist = sorted(cmds.keys())

    parser.add_argument(
        'action',
        metavar='action',
        nargs='?',
        default='help',
        choices=cmdlist,
        help='Action is one of ' + ', '.join(cmdlist))

    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='enable verbose')

    if add_arguments:
        add_arguments(parser)

    args = parser.parse_args()

    if args.verbose:
        os.environ['V'] = '1'
    cmds[args.action]()


def depsched_intro():
    """Print to the stdout the program banner."""
    print("""
       .___                         .__               .___
     __| _/____ ______  ______ ____ |  |__   ____   __| _/
    / __ |/ __ \\\\____ \/  ___// ___\|  |  \_/ __ \ / __ |
   / /_/ \  ___/|  |_> >___ \\\\ \___ |   Y  \  ___// /_/ |
   \____ |\___  >   __/____  >\___  >___|  /\___  >____ |
        \/    \/|__|       \/     \/     \/     \/     \/ """)


def cmd(cmd, quiet=False):
    """Executes a subprocess running a shell command and returns the output."""
    if quiet:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True)
    else:
        proc = subprocess.Popen(cmd, shell=True)

    out, _ = proc.communicate()

    if proc.returncode:
        if quiet:
            print('Log:\n', out, file=sys.stderr)
        print('Error has occurred running command: %s' % cmd, file=sys.stderr)
        sys.exit(proc.returncode)
    return out
