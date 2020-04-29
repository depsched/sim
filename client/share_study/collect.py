#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import print_function

import pprint as pp
import sys

import requests
from pymongo import MongoClient

import hub
from utils import Timer, ts_gen
from imagedb import BasicImageDB


"""
Collect layer sharing info in image manifest
    - Storage backend with mongodb gives ACID and scaling
    - Crawler frontend with requests exploits parallelism
"""


class Fetcher(BasicImageDB):
    def __init__(self):
        self._session_auth = requests.Session()
        self._session_registry = requests.Session()
        self._session_store = requests.Session()
        # image database initialization
        self.connect_db()
        self.fetch_status = self.image_db.fetch_status

    def start(self):
        if self.fetch_status.find_one() is None:
            self.fetch_status.insert_one({"checkpoint": "1"})
        self.resume()

    def resume(self):
        checkpoint = int(self.fetch_status.find_one()["checkpoint"])
        self.populate_db(start_page=checkpoint)

    def populate_db(self,
                    start_page=1,
                    end_page=10000,
                    page_size=100,
                    ):
        """
        Fetch and insert image info into the database, with "latest" tag only
        """
        timestamp, timer = ts_gen(), Timer()
        for i in range(end_page + 1):
            if i < start_page:
                continue

            # get repositories; for each repository get a access token
            repositories = hub.get_repositories(i, page_size, self._session_store)
            for repository in repositories:
                repo_name, popularity = repository[0], repository[1]

                # fetching digests using the same session
                # timer.start()
                token = hub.get_repo_token(repo_name, self._session_auth)
                hub.update_session_token(self._session_registry, token)
                digests = hub.get_digests(repo_name, "latest", self._session_registry)
                # timer.stop_and_report("remote request")

                # timer.start()
                self.images.update({"name": repo_name},
                                   {"name": repo_name,
                                    "popularity": popularity,
                                    "timestamp": timestamp,
                                    "tags": {"latest": {"layers": digests}},
                                    },
                                   upsert=True)
                # timer.stop_and_report("database update")
                pp.pprint("--> {} images have been collected so far.".format(self.images.count()))

            # checkpoint progress by page number
            self.fetch_status.update({"checkpoint": {"$regex": ".*"}},
                                     {"checkpoint": str(i)},
                                     )

    def start_at(self, start_page):
        checkpoint = self.fetch_status.find_one()["checkpoint"]
        if checkpoint != None and int(checkpoint) > start_page:
            pp.pprint("--> starting page is earlier than the checkpoint ({}),"
                      " continue..".format(int(checkpoint)))
            # " press any key to continue.".format(int(checkpoint)))
            # input()
        self.populate_db(start_page=start_page, end_page=start_page + 100)
        pp.pprint("--> Completed at page {}.".format(start_page + 100))


def main():
    f = Fetcher()
    if len(sys.argv) > 1:
        start_page = int(sys.argv[1])
        pp.pprint("--> starting at page {}, fetching next 100 pages.".format(start_page))
        f.start_at(start_page)
    f.start()


if __name__ == "__main__":
    main()
