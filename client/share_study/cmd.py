#!/usr/bin/env python3

from __future__ import print_function

import argparse
import os
import os.path
import sys
from multiprocessing import Process

import analyze
import time
from utils import cmd


def _collect():
    """
    As of Feb. 2018, 1600 * 100 images account for 10% of total public images on docker hub
    """
    end_page, page_size = 2000, 100
    counter, gap = 0, 8
    for start_page in range(1400, end_page, page_size):
        counter += 1
        if counter % gap == 0:
            time.sleep(3600)
        cmd("python3 collect.py {} &".format(start_page))


def _patch():
    start, step_size, step_count = 80000, 10000, 16
    for step in [start, step_size * step_count, step_size]:
        p = Process(target=_patch_func, args=(step,))
        p.start()


def _patch_func(step):
    analyzer = analyze.Analyzer()
    analyzer.patch_layer_size_async(start_rank=step, least_share_count=0)


def _plot():
    pass


def main():
    parser = argparse.ArgumentParser(description='Collector cmds.')
    cmds = {
        'plot': _plot,
        'collect': _collect,
        'patch': _patch,
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
        '-v',
        '--verbose',
        action='store_true',
        help='enable verbose')

    args = parser.parse_args()

    if args.verbose:
        os.environ['V'] = '1'
    cmds[args.action]()


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
        print('--> Helped you change to the correct directory...')
        os.chdir(fdir)


def _intro():
    print("""
       .___                         .__               .___
     __| _/____ ______  ______ ____ |  |__   ____   __| _/
    / __ |/ __ \\\\____ \/  ___// ___\|  |  \_/ __ \ / __ |
   / /_/ \  ___/|  |_> >___ \\\\ \___ |   Y  \  ___// /_/ |
   \____ |\___  >   __/____  >\___  >___|  /\___  >____ |
        \/    \/|__|       \/     \/     \/     \/     \/ 
        @trace collect.""")


if __name__ == '__main__':
    _check_root()
    _check_run_dir()
    _intro()

    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit('\nInterrupted')
