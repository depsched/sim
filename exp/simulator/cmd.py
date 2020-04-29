#!/usr/bin/env python3

import argparse
import os
import os.path
import subprocess
import sys

from . import simulator
from .experiments import *


def _sim(args):
    exps = {"cont_cap": args.cont_cap,
            "cont_length": args.cont_length,
            "store_size": args.store_size,
            "lat_cdf": args.lat_cdf,
            "req_rate": args.req_rate,
            "evict_dep": args.evict_dep,
            "precached": args.precached,
            "delay_sched": args.delay_sched,
            }

    if args.all:
        _clear()
        for exp in exps.keys():
            exp_routine(exp)

    elif True in exps.values():
        for exp, run in exps.items():
            if run:
                _clear(exp)
                if exp == "precached":
                    exp_routine(exp, uniform=False)
                else:
                    exp_routine(exp)
    else:
        _sim_single()


def _sim_single():
    s = simulator.Simulator()
    s.sim(**default_params)


def _clear(exp=None):
    if exp is None:
        cmd("rm -rf ./plot/")
        cmd("rm -rf ./result/")
    else:
        cmd("rm -f ./result/meta_{}.txt".format(exp))
        cmd("rm -rf ./result/run_{}".format(exp))


def main():
    parser = argparse.ArgumentParser(description='Simulator cmds.')
    cmds = {
        'sim': _sim,
        'clear': _clear,
        'argtest': _arg_test,
        'help': lambda: _print_usage(parser),
    }

    for name in cmds.keys():
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
        '-c',
        '--cont_cap',
        action='store_true',
        help='container cap param sweep')
    parser.add_argument(
        '-l',
        '--cont_length',
        action='store_true',
        help='container length param sweep')
    parser.add_argument(
        '-s',
        '--store_size',
        action='store_true',
        help='layer store size param sweep')
    parser.add_argument(
        '-r',
        '--req_rate',
        action='store_true',
        help='request rate param sweep')
    parser.add_argument(
        '-t',
        '--lat_cdf',
        action='store_true',
        help='latency cdf')
    parser.add_argument('-a', '--all', action='store_true',
                        help='all predefined simulation cases')
    parser.add_argument(
        '-z',
        '--evict',
        action='store_true',
        help='comparing eviction')
    parser.add_argument(
        '-e',
        '--evict_dep',
        action='store_true',
        help='comparing eviction policies')
    parser.add_argument(
        '-d',
        '--delay_sched',
        action='store_true',
        help='comparing eviction policies')
    parser.add_argument(
        '-p',
        '--precached',
        action='store_true',
        help='precached experiment.')
    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='enable verbose')

    args = parser.parse_args()

    if args.verbose:
        os.environ['V'] = '1'
    if args.action == "sim":
        cmds[args.action](args)
    else:
        cmds[args.action]()


def cmd(cmd, quiet=False):
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
        print('Error has occured running command: %s' % cmd, file=sys.stderr)
        sys.exit(proc.returncode)


def cmd_out(cmd):
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=True)

    out, _ = proc.communicate()

    if proc.returncode:
        print('Error occured running host command: %s' % cmd, file=sys.stderr)
        sys.exit(proc.returncode)

    return out


def cmd_ignore(cmd):
    proc = subprocess.Popen(cmd, shell=True)
    proc.communicate()

    if proc.returncode:
        print('Error occured running host command: %s' % cmd, file=sys.stderr)
        print('Ignored, process continues..')


def cmd_success(cmd):
    try:
        subprocess.check_call(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True)
        return True
    except subprocess.CalledProcessError:
        return False


def _print_usage(parser):
    parser.print_help(file=sys.stderr)
    sys.exit(2)


def _arg_test():
    print('halo, arg arg.')


def _check_root():
    euid = os.geteuid()
    if euid != 0:
        print('--> Please run the script as root...')
        exit()


def _check_run_dir():
    cwd = os.getcwd()
    fabspath = os.path.abspath(__file__)
    fdir = os.path.dirname(fabspath)

    if cwd != fdir:
        print('--> helped you change to the correct directory...')
        os.chdir(fdir)


def _intro():
    print("""
       .___                         .__               .___
     __| _/____ ______  ______ ____ |  |__   ____   __| _/
    / __ |/ __ \\\\____ \/  ___// ___\|  |  \_/ __ \ / __ |
   / /_/ \  ___/|  |_> >___ \\\\ \___ |   Y  \  ___// /_/ |
   \____ |\___  >   __/____  >\___  >___|  /\___  >____ |
        \/    \/|__|       \/     \/     \/     \/     \/ """)


if __name__ == '__main__':
    _check_root()
    _check_run_dir()
    _intro()

    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit('\nInterrupted')
