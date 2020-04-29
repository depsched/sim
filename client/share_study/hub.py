#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import print_function

import asyncio
import json

import aiohttp
import gevent.monkey
import requests
from time import sleep

gevent.monkey.patch_socket()
from gevent.pool import Pool

'''
Emulate a docker pull command to fetch image and layer info (or the real thing) from dockerhub
    - Supports Docker Registry API HTTP V2 (dockerhub default)
    - Supports DockerHub repositories metadata 
    
Each image is uniquely identified by its repository and tag, e.g., library/docker:latest    
    - Format: {(repo)[organization]/[image name]}:[tag]
    - Layers of all images under the same repo are stored under the same repo prefix
    
The endpoints/URI prefixes used are in what follows:
'''

EP_REGISTRY = "https://registry.hub.docker.com/v2/"
EP_REPO_TOKEN = "https://auth.docker.io/token?service=registry.docker.io&scope=repository:{}:pull"
EP_STORE = "https://store.docker.com/api/content/v1/products/search?" \
           "page_size={page_size}&page={page}&q=%2B&source=community&type=image%2Cbundle"


def get_repositories(page=1, page_size=100, session=None):
    """
    Get a list of repositories from a page of docker store, sorted by popularity;
    The maximum page size supported by docker store is 100 (thus the default value).

    :return: list of tuples (image, popularity...)
    """
    repo_list_ep = EP_STORE.format(page=page, page_size=page_size)
    if session is not None:
        response = session.get(
            repo_list_ep,
            json=True,
        ).json()
    else:
        response = requests.get(
            repo_list_ep,
            json=True,
        ).json()

    return [(entry["name"], entry["popularity"]) for entry in response["summaries"]]


def get_tags(repo, session=None):
    tag_ep = EP_REGISTRY + "{}/tags/list".format(repo)
    if session is not None:
        assert "Authorization" in session.headers
        response = session.get(
            tag_ep,
        ).json()
    else:
        token = get_repo_token(repo)

        response = requests.get(
            tag_ep,
            headers={"Authorization": "Bearer {}".format(token)},
        ).json()
    return response["tags"] if "tags" in response else []

def download_layer(repo, digest, session=None):
    """
    Download layer to current directory; for all functions in this module,
    repository uniquely identifies the image, and can thus be used as the
    image name/identifier

    :param repo:
    :param digest:
    :param session:
    :return:
    """
    response = _get_layer(repo, digest, session)

    with open("./" + digest + ".tar", 'wb') as f:
        f.write(response.content)


def get_layer_size(repo, digest, session=None):
    """
    Get the layer size in bytes (compressed).

    :param repo: str, repository name
    :param digest: str, layer digest
    :param session: obj, requests session to reuse (TCP) connection
    :return: int, layer in bytes
    """
    response = _get_layer(repo, digest, session, header_only=True)
    # return the connection back to the session's connection pool
    response.close()
    return int(response.headers["Content-Length"])


def get_repo_token(repo, session=None):
    """
    GET pull token from dockerhub

    :param repo:
    :return:
    """
    if session is not None:
        return session.get(EP_REPO_TOKEN.format(repo), json=True).json()["token"]
    else:
        return requests.get(EP_REPO_TOKEN.format(repo), json=True).json()["token"]


def update_session_token(session, token):
    session.headers.update({"Authorization": "Bearer {}".format(token)})


def get_digests(repo, tag, session=None):
    """
    Get a list of layer digests of the image

    :param repo: str, repository/image name
    :param tag:
    :return:
    """
    return _ext_digests(_get_manifest(repo, tag, session))


def _get_manifest(repo, tag, session=None):
    """
    Emulate a docker pull request to fetch the manifest file of an image

    :param repo: str, name of the image repository
    :param tag: str, image tag, if any
    :return: dict, manifest file in json
    """
    manifest_ep = EP_REGISTRY + "{repository}/manifests/{tag}"

    if session is not None:
        return session.get(
            manifest_ep.format(repository=repo, tag=tag),
            json=True
        ).json()
    else:
        token = get_repo_token(repo)

        return requests.get(
            manifest_ep.format(repository=repo, tag=tag),
            headers={"Authorization": "Bearer {}".format(token)},
            json=True
        ).json()


def _get_layer(repo, digest, session=None, header_only=False):
    """
    Emulate a docker pull layer request, if header is true, return only the header
    - docker hub does not respond with Content-Length field with the HTTP HEAD request,
      therefore we use GET request here but defer downloading the body
    - reuse the session, assuming the token field is set

    :param repo: str, repository name
    :param digest: str, layer digest
    :param session: obj, if not None, reuse the session
    :param header: bool, true if only header is wanted
    :return: obj, response
    """
    # endpoint for layer blob
    layer_ep = EP_REGISTRY + "{repository}/blobs/{digest}"

    if session is not None:
        assert "Authorization" in session.headers

        response = session.get(
            layer_ep.format(repository=repo, digest=digest),
            stream=header_only,
        )
    else:
        token = get_repo_token(repo)

        response = requests.get(
            layer_ep.format(repository=repo, digest=digest),
            headers={"Authorization": "Bearer {}".format(token)},
            stream=header_only,
        )
    return response


def _ext_digests(manifest):
    """
    From an image manifest, extract a (de-duplicated) list of layer digests

    :param manifest: image manifest in json
    :return: a list of layer digests in the image
    """
    try:
        return list(set([l["blobSum"] for l in manifest["fsLayers"]]))
    except KeyError:
        return list()


def print_json(string):
    print(json.dumps(string, indent=4, sort_keys=True))


def get_batch_request_pool(urls, session):
    request_pool = Pool(len(urls))
    requests_ = []
    for url in urls:
        requests_.append(request_pool.spawn(session.get, url))
    request_pool.join()
    return [r.value.json() for r in requests_]


def get_layer_size_batch_request_pool(repo, digests, session=None):
    """This version uses gevent.pool, obsolete"""
    layer_ep = EP_REGISTRY + "{repository}/blobs/{digest}"
    request_pool = Pool(10)
    requests_ = []
    for i in range(len(digests)):
        requests_.append(request_pool.spawn(session.get, layer_ep.format(repository=repo, digest=digests[i])))
    request_pool.join()
    return [r.value for r in requests_]


def get_layer_size_batch(repo_digest_pairs):
    layer_ep, urls, repositories = EP_REGISTRY + "{repository}/blobs/{digest}", [], []
    for repo, digest in repo_digest_pairs:
        urls.append(layer_ep.format(repository=repo, digest=digest))
        repositories.append(repo)
    # async requests
    futures = [_get_layer_size_async(url, repo) for url, repo in zip(urls, repositories)]
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(asyncio.gather(*futures))
    loop.run_until_complete(asyncio.sleep(0))
    return results


async def _get_layer_size_async(url, repo):
    async with aiohttp.ClientSession() as session:
        # get and load token
        response = await session.get(EP_REPO_TOKEN.format(repo), json=True)
        token = json.loads(await response.text())["token"]
        headers = {"Authorization": "Bearer {}".format(token)}
        # get manifests
        response = await session.get(url, headers=headers)
        size = response.headers["Content-Length"]
        return size


def get_digests_batch(images):
    """
    :param images: a list of images in full name
    :return: a list of layer digests
    """
    manifest_ep, urls, repositories = EP_REGISTRY + "{repository}/manifests/{tag}", [], []
    # extract url and repo
    for image in images:
        image = image.split(":")
        repo, tag = image[0], image[1]
        urls.append(manifest_ep.format(repository=repo, tag=tag))
        repositories.append(repo)
    # request for manifest
    futures = [_get_manifest_async(url, repo) for url, repo in zip(urls, repositories)]
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(asyncio.gather(*futures))
    return [_ext_digests(result) for result in results]


async def _get_manifest_async(url, repo, token=None):
    async with aiohttp.ClientSession() as session:
        # get and load token
        if not token:
            response = await session.get(EP_REPO_TOKEN.format(repo), json=True)
            token = json.loads(await response.text())["token"]
        headers = {"Authorization": "Bearer {}".format(token)}
        # get manifests
        response = await session.get(url, headers=headers)
        manifest = json.loads(await response.text())
        return manifest

async def get_digests_func(repo, tag, token=None):
    manifest_ep = EP_REGISTRY + "{repository}/manifests/{tag}"
    url = manifest_ep.format(repository=repo, tag=tag)
    return _ext_digests(await _get_manifest_async(url, repo, token))


async def get_token_async(repo):
    async with aiohttp.ClientSession() as session:
        # get and load token
        response = await session.get(EP_REPO_TOKEN.format(repo), json=True)
        return json.loads(await response.text())["token"]


if __name__ == "__main__":
    # print_json(_get_manifest("library/gcc", "latest"))
    # print(get_layer_size("library/gcc", "sha256:94ca74ebc22fca68173d9716d1891d1a44484a942f471609a1832ac3ea435454"))
    # print(get_layer_size("cfbuildpacks/ci", "sha256:adbc9ddfca8578ced7b5879357028ac444363c02267465866d54c672c97f0477"))
    # print(get_digests_batch(
    #     ["library/gcc:latest", "library/nginx:latest", "library/redis:latest", "library/docker:latest"]))
    # print(get_layer_size_batch(
    #     [("library/gcc", "sha256:94ca74ebc22fca68173d9716d1891d1a44484a942f471609a1832ac3ea435454")]))
    print(_get_manifest("nginx","latest"))
