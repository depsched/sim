import pprint as pp
import subprocess
import sys
import time
import argparse
import os

mb = 1.024 * pow(10, 6)
gb = mb * pow(10, 3)
dir_path = os.path.dirname(os.path.realpath(__file__))

class Timer():
    def start(self):
        self._start_tick = time.time()

    def stop(self):
        self._stop_tick = time.time()

    def stop_and_report(self, banner="", precision=4):
        self.stop()
        elapsed_time = round(self._stop_tick - self._start_tick, precision)
        if banner != "":
            pp.pprint("--> {} took {} seconds.".format(banner, elapsed_time))
        return elapsed_time


def ts_gen():
    return time.strftime("%Y%m%d%H%M%S", time.localtime())


def time_func(f):
    def f_timed(*args, **kwargs):
        start = time.time()
        result = f(*args, **kwargs)
        end = time.time()
        elapsed_time = end - start
        print("--> {function} took {time} seconds.".format(function=f.__name__, time=elapsed_time))
        return result

    return f_timed


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