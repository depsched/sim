#!/usr/bin/env python

from __future__ import print_function
import sys
import os
import os.path
import re
import subprocess
import argparse

def cmd(cmd, quiet=False):
    if quiet:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    else:
        proc = subprocess.Popen(cmd, shell=True)

    out, _ = proc.communicate()

    if proc.returncode:
        if quiet:
            print('Log:\n', out, file=sys.stderr)
        print('Error has occured running command: %s' % cmd, file=sys.stderr)
        sys.exit(proc.returncode)

def cmd_success(cmd):
    try:
        subprocess.check_call(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        return True
    except subprocess.CalledProcessError:
        return False

def print_usage(parser):
    parser.print_help(file=sys.stderr)
    sys.exit(2)

def arg_test():
    print('halo, arg arg.')

def main():
    parser = argparse.ArgumentParser(description='Template cmd wrapper.')
    cmds = {
            'argtest': arg_test,
            'help': lambda: print_usage(parser),
            }

    for name in cmds.keys():
        if '_' in name:
            cmds[name.replace('_','-')] = cmds[name]

    cmdlist = sorted(cmds.keys())

    parser.add_argument(
        'action',
        metavar='action',
        nargs='?',
        default='argtest',
        choices=cmdlist,
        help='Action is one of ' + ', '.join(cmdlist))

    parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose')

    args = parser.parse_args()

    if args.verbose:
        os.environ['V'] = '1'

    cmds[args.action]()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit('\nInterrupted')
