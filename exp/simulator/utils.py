#!/usr/bin/env python3

import argparse
import errno
import os
import random
import string
import subprocess
import sys
import time

mb = 1.024 * pow(10, 6)
gb = mb * pow(10, 3)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def dir_check(path):
    """check if the dir exists; create it if not;
    return the dir name in either case"""
    try:
        os.makedirs(path)
        return path
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            print("==> cannot create directory.")
            sys.exit(1)
        else:
            return path


def timed(method):
    def timeit(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.9f ms' % (method.__name__, (te - ts) * 1000))
        return result

    return timeit


def time_func(f):
    def f_timed(*args, **kwargs):
        start = time.time()
        result = f(*args, **kwargs)
        end = time.time()
        elapsed_time = end - start
        print("--> {function} took {time} seconds.".format(function=f.__name__, time=elapsed_time))
        return result

    return f_timed


def id_gen(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def ts_gen():
    return time.strftime("%Y%m%d%H%M%S", time.localtime())


def banner(topic):
    splitter = "-" * 10
    print("\n{}|{}|{}".format(splitter, topic, splitter))


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


def cmd_ignore_error(cmd):
    """Executes a subprocess running a shell command; reports but ignores any errors."""
    proc = subprocess.Popen(cmd, shell=True)
    proc.communicate()

    if proc.returncode:
        print('Error occurred running host command: %s' % cmd, file=sys.stderr)
        print('Ignored, process continues..')


def cmd_check_success(cmd):
    """Executes a subprocess running a shell command; checks its run."""
    try:
        subprocess.check_call(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True)
        return True
    except subprocess.CalledProcessError:
        return False


def _unit_test():
    pass


def main():
    _unit_test()


if __name__ == "__main__":
    main()
