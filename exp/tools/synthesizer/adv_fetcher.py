#!/usr/bin/env python

from __future__ import print_function
from collections import defaultdict
import sys, os, os.path, re, subprocess, argparse
import requests, json, const
from fetcher import Fetcher

sys.path.append("../../layerinfo/")
import digest as dg

class AdvFetcher(Fetcher):
    def __init__(self):
        Fetcher.__init__(self)
        '''
        Store image stats, dict
        key: str, image name, none library images named as repo/image_name
        value: list, [image_name, star_count, pull_count, #_layers, image_size]
        '''
        self.image_stat_map = defaultdict(lambda: [None, 0, 0, 0, 0])
        '''
        Store layer stats, dict
        key: str, layer digest
        value: list, [layer_digest, star_count, pull_count, #_owners, layer_size, owner_list]
        '''
        self.layer_stat_map = defaultdict(lambda: [None, 0, 0, 0, 0,[]])
        '''
        Store layer size, dict
        key: str, layer digest
        value: int, layer size in bytes
        '''
        self.layer_size_map = defaultdict(int)

    def fetch(self):
        print("--> fetching official images..")
        self.get_lib_repo()

        print("--> fetching defined images..")
        self.get_cust_repo()

        print("--> fetching image layers info..")
        self.get_image_layer_map()

        print("--> fetching layer size info..")
        self.get_layer_size_info()

        print("--> compute layer stats..")
        self.reduce_layer_stat_map()

        print("--> dumping image stats to file..")
        self.dump_image_stat()

        print("--> dumping layer stats to file..")
        self.dump_layer_stat()

        print("--> fetch completed.")

    def get_lib_repo(self):
        next_ep = "https://hub.docker.com/v2/repositories/library/?page=1"
        cur_page = requests.get(next_ep, json=True).json()
        self.count = cur_page["count"]

        while next_ep is not None:
            results = cur_page["results"]
            for image in results:
                stat = self.image_stat_map["library/" + image["name"]]
                stat[0] = image["name"]
                stat[1] = image["star_count"]
                stat[2] = image["pull_count"]

            cur_page = requests.get(next_ep, json=True).json()
            next_ep = cur_page["next"]

    def get_cust_repo(self):
        for repo in self.cust_repo_list:
            next_ep = "https://hub.docker.com/v2/repositories/" + repo + "?page=1"

            while next_ep is not None:
                cur_page = requests.get(next_ep, json=True).json()
                image_stat_list = [[i["name"],i["star_count"],i["pull_count"]] for i in cur_page["results"]]
                for i in image_stat_list:
                    stat = self.image_stat_map[repo + i[0]]
                    stat[0] = repo + i[0]
                    stat[1] = i[1]
                    stat[2] = i[2]
                next_ep = cur_page["next"]

    def get_layer_size_info(self):
        images = self.image_stat_map.keys()

        batch_size = const.batch_size
        for i in xrange(0, len(images), batch_size):
            batch = images[i:i+batch_size]
            self.pull_image_batch(batch)

            r = cmd_out("./layerinfo_client")
            for l in r.rstrip().split("\n"):
                l = l.split()
                self.layer_size_map[l[0]] = int(l[1])
        #print(json.dumps(self.layer_size_map, indent=4))

    def reduce_layer_stat_map(self):
        for i, layers in self.image_layer_map.items():
            for l in layers:
                # check if hit
                if self.layer_stat_map[l][0] != None:
                    self.layer_hit_map[l] += 1

                # sum up the star_count, pull_count of the layer's each parent image
                self.layer_stat_map[l][0] = l
                self.layer_stat_map[l][1] += self.image_stat_map[i][1]
                self.layer_stat_map[l][2] += self.image_stat_map[i][2]

                if l in self.layer_size_map:
                    self.layer_stat_map[l][4] = self.layer_size_map[l]

                self.layer_stat_map[l][5].append(i)

                # piggyback the size count for image stat here
                self.image_stat_map[i][4] += self.layer_size_map[l]

    def dump_image_stat(self):
        open(self.out_file_image, 'w').close()

        with open(self.out_file_image, 'a') as f:
            for i in self.image_stat_map:
                stat = self.image_stat_map[i]
                stat[3] = self.image_layer_count_map[i]
                f.write(','.join(map(str,stat)) + "\n")

    def dump_layer_stat(self):
        open(self.out_file_layer, 'w').close()

        with open(self.out_file_layer, 'a') as f:
            for l in self.layer_stat_map:
                stat = self.layer_stat_map[l]
                stat[3] = self.layer_hit_map[l] + 1
                stat[5] = '|'.join(stat[5])
                f.write(','.join(map(str,stat)) + "\n")

    def pull_image_batch(self, images):
        self.clean_local_repo()
        for i in images:
            cmd_ignore("docker pull " + i)

    def clean_local_repo(self):
        cmd_ignore("""docker stop $(docker ps -aq);
                      docker rm $(docker ps -aq);
                      docker rmi $(docker images -aq)""")

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

def cmd_ignore(cmd):
    proc = subprocess.Popen(cmd, shell=True)
    _, _ = proc.communicate()

def cmd_out(cmd):
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)

    out, _ = proc.communicate()

    if proc.returncode:
        print('Error occured running host command: %s' % cmd, file=sys.stderr)
        sys.exit(proc.returncode)
    return out

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

def fetch():
    f = AdvFetcher()
    f.fetch()

def main():
    parser = argparse.ArgumentParser(description='Template cmd wrapper.')
    cmds = {
            'argtest': arg_test,
            'fetch': fetch,
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
        default='fetch',
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
