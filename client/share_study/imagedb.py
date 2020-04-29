#!/usr/bin/env python3
"""Basic image database."""
import os

from pymongo import MongoClient

# from .__cred__.cred import MONGO_USER, MONGO_PASSWD
MONGO_USER = ""
MONGO_PASSWD = ""
from .utils import cmd, dir_path

IMAGE_DB_PATH = "/data"
IMAGE_DB_NAME = "/depsched_db"
OFFICIAL_IMAGES_FILE = dir_path + "/__result__/official_image_list.csv"
OFFICIAL_IMAGE_STATS = dir_path + "/__result__/official_image_stats.csv"
OFFICIAL_LAYER_STATS = dir_path + "/__result__/official_image_stats.csv"


class BasicImageDB():
    """Manage both access to EC2 repository and local database.

    The methods assume the repository name on ECR is equivalent to the image
    name, i.e., they consider the "latest" tag of an image only.
    """

    def __init__(self):
        """The database client is lazily created. Use connect_local_db for
        active creation."""
        pass

    def __getattr__(self, name):
        """Allows lazily connection to the database."""
        assert name in {"db_client", "image_db", "images", "layers"}, \
            "missing attribute: {}; not allowed for lazy creation.".format(name)
        self.connect_db()
        return self.__dict__[name]

    def connect_db(self):
        """Create database client and references."""
        self.db_client = MongoClient("localhost",
                                     27017,
                                     username=MONGO_USER,
                                     password=MONGO_PASSWD,
                                     authSource="image_db",
                                     authMechanism="SCRAM-SHA-1",
                                     )
        self.image_db = self.db_client.image_db
        self.images = self.image_db.images
        self.layers = self.image_db.layers
        print("--> connected to image db.")
        return self.db_client

    def get_top_images(self, rank=1000):
        """Get a list of top popular images with the default tag."""
        images = self.images.find().sort("popularity", -1).limit(rank)
        return [image["name"] for image in images]

    def start_db_daemon(self):
        cmd("mongod --auth --port 27017 --dbpath /data/{} "
            ">> /tmp/mongo.log 2>&1 &".format(IMAGE_DB_NAME))

    def get_duplicate_names(self, rank=1000):
        images = self.images.find().sort("popularity", -1).limit(rank)
        counter, checked = 0, set()
        for entry in images:
            full_name = entry["name"]
            image_name = full_name.split("/")[1]
            if image_name in checked:
                counter += 1
            else:
                checked.add(image_name)
        return counter


def load_official_images():
    """Load from a data file the official images names."""
    official_images = set()
    with open(OFFICIAL_IMAGES_FILE, "r") as f:
        for line in f.readlines():
            image_name = line.rstrip().split("/")[1]
            official_images.add(image_name)
    return list(official_images)


def load_official_image_stats():
    """Return a dict of image stats."""
    official_images = dict()
    with open(OFFICIAL_IMAGE_STATS, "r") as f:
        for line in f.readlines():
            image_stat = line.rstrip().split(",")
            image_name = image_stat[0]
            official_images[image_name] = {"popularity": int(image_stat[2])}
    return official_images


if __name__ == "__main__":
    db = BasicImageDB()
    print(db.get_duplicate_names(1000))
