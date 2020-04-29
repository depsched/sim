#!/usr/bin/env python

import requests
import json

'''
Emulate a docker pull command to fetch image manifest, layer digests and so on
    - Supports Docker Registry API HTTP V2 (dockerhub default)
'''

def get_layer(repo, digest):
    # GET pull token from docker hub
    login_template = "https://auth.docker.io/token?service=registry.docker.io&scope=repository:{repository}:pull"
    token = requests.get(login_template.format(repository=repo), json=True).json()["token"]

    # GET layer
    get_layer_template = "https://registry.hub.docker.com/v2/{repository}/blobs/{digest}"
    r = requests.get(
        get_layer_template.format(repository=repo, digest=digest),
        headers={"Authorization": "Bearer {}".format(token)},
    )

    with open("./"+digest+".tar", 'wb') as f:
        f.write(r.content)

if __name__ == "__main__":
    get_layer("library/nginx", "sha256:bc95e04b23c06ba1b9bf092d07d1493177b218e0340bd2ed49dac351c1e34313")
