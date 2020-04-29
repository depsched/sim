#!/usr/bin/env python

import requests
import json

'''
Emulate a docker pull command to fetch image manifest, layer digests and so on
    - Supports Docker Registry API HTTP V2 (dockerhub default)
'''

def get_manif(repo, tag):
    # GET pull token from docker hub
    login_template = "https://auth.docker.io/token?service=registry.docker.io&scope=repository:{repository}:pull"
    token = requests.get(login_template.format(repository=repo), json=True).json()["token"]

    # GET manifest
    get_manifest_template = "https://registry.hub.docker.com/v2/{repository}/manifests/{tag}"
    return requests.get(
        get_manifest_template.format(repository=repo, tag=tag),
        headers={"Authorization": "Bearer {}".format(token)},
        json=True
    ).json()

def ext_digest(manif):
    # obtain a (deduplicated) list of layer digests
    return list(set([l["blobSum"] for l in manif["fsLayers"]]))

def get_digest(repo="library/gcc", tag="latest"):
    print_json(get_manif(repo,tag))
    return ext_digest(get_manif(repo, tag))

def print_json(string):
    print json.dumps(string, indent=4, sort_keys=True)

if __name__ == "__main__":
    print get_digest()
