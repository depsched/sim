#!/usr/bin/env python

from __future__ import print_function
from collections import defaultdict
import sys, os, os.path, re, subprocess, argparse
import requests, json, const

sys.path.append("../../layerinfo/")
import digest as dg

class Fetcher:
    def __init__(self):
        self.count = 0
        self.cust_repo_list = const.cust_repo_list
        '''
        Output csv formats:
        image stats file: image_name,star_count,pull_count,layers_count
        layer stats file: layer_digest,star_count,pull_count,parents_count
        '''
        self.out_file_image = const.out_file_image
        self.out_file_layer = const.out_file_layer
        '''
        Store image stats, dict
        key: str, image name, none library images named as repo/image_name
        value: tuple, (image_name, star_count, pull_count, layers_count)
        '''
        self.image_stat_map = dict()
        '''
        Store image-layer mapping, dict
        key: str, image name
        value: str[], layer_digest list
        '''
        self.image_layer_map = dict()
        '''
        Store layer stats, dict
        key: str, layer digest
        value: tuple, (layer_digest, star_count, pull_count, parents_count)
        '''
        self.layer_stat_map = defaultdict(lambda: [None, 0, 0])
        '''
        Store invalid image, list
        element: str, image_name
        '''
        self.black_image_list = list()
        '''
        Store layer hit count, i.e., how many parents a layer has, dict
        key: str, layer digest
        value: int, parent count
        '''
        self.layer_hit_map = defaultdict(int)
        self.image_layer_count_map = defaultdict(int)

    def fetch(self):
        print("--> fetching official images..")
        self.get_lib_repo()
        print("--> fetching defined images..")
        self.get_cust_repo()
        print("--> fetching image layers info..")
        self.get_image_layer_map()
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
                name = image["name"]
                star_count = image["star_count"]
                pull_count = image["pull_count"]
                self.image_stat_map["library/" + name] = ["library/" + name, star_count, pull_count]
            cur_page = requests.get(next_ep, json=True).json()
            next_ep = cur_page["next"]

        #print(self.image_stat_map, self.image_stat_map["nginx"], len(self.image_stat_map))

    def get_cust_repo(self):
        for repo in self.cust_repo_list:
            next_ep = "https://hub.docker.com/v2/repositories/" + repo + "?page=1"

            while next_ep is not None:
                cur_page = requests.get(next_ep, json=True).json()
                image_stat_list = [[i["name"],i["star_count"],i["pull_count"]] for i in cur_page["results"]]
                for i in image_stat_list:
                    self.image_stat_map[repo + i[0]] = [repo + i[0], i[1], i[2]]
                next_ep = cur_page["next"]

    def get_image_layer_map(self):
        for i, _ in self.image_stat_map.items():
            print("--> fetching layers of ", i)
            try:
                layers = dg.get_digest(i, tag="latest")
                self.image_layer_map[i] = layers
                print("--> image %s has layers: " % i, str(layers))
                self.image_layer_count_map[i] += len(layers)
            except:
                print("--> invalid image found: %s, remove from maps later" % i)
                self.black_image_list.append(i)

        for i in self.black_image_list:
            del self.image_stat_map[i]

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

    def dump_image_stat(self):
        open(self.out_file_image, 'w').close()

        with open(self.out_file_image, 'a') as f:
            for i, stat in self.image_stat_map.iteritems():
                stat = list(stat)
                stat.append(self.image_layer_count_map[i])
                f.write(','.join([str(i) for i in stat]) + "\n")

    def dump_layer_stat(self):
        open(self.out_file_layer, 'w').close()

        with open(self.out_file_layer, 'a') as f:
            for l, stat in self.layer_stat_map.iteritems():
                stat = list(stat)
                stat.append(self.layer_hit_map[l])
                f.write(','.join([str(i) for i in stat]) + "\n")

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

def fetch():
    f = Fetcher()
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
