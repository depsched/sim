import asyncio
import pprint as pp
import sys

import numpy as np
import pymongo
import requests

import hub
from imagedb import BasicImageDB
from utils import ts_gen, Timer, mb

"""
- when add new database queries, you may need to benchmark to see if optimization is needed
- avoid writing actions that takes substantial operations between db client and server
- consider in-memory processing first 
    - when ACID does not matter, dump to db in one operation at the end
    - unlike the populate_db in collect.py, where the network IO is the bottleneck and order 
    of magnitude slower and persisting data to the disk is important; here, disk IO is the 
    bottleneck
"""


class Analyzer(BasicImageDB):
    def __init__(self):
        self.connect_db()
        super().__init__()

    def query(self):
        self._query_layer_count()
        self._query_popularity()

    def index(self):
        print("--> indexing..")
        self.images.create_index([("popularity", pymongo.DESCENDING),
                                  ("name", pymongo.TEXT)])
        self.layers.create_index([("popularity", pymongo.DESCENDING),
                                  ("share_count", pymongo.DESCENDING),
                                  ("digest", pymongo.TEXT)])
        print("--> done.")

    def patch(self):
        self.patch_image_depth()
        self.patch_invert_in_memory()
        self.patch_layer_share_count()

    def query_popularity(self):
        results = self.images.find({"popularity": {"$gt": 10000}}).count()
        # results = self.images.find({"popularity": {"$gt": 10000}}).sort("popularity", -1)

    def query_layer_count(self):
        return self.layers.find({}).count()

    def query_image_versions(self, least_rank=100):
        images = self.images.find().sort("popularity", -1).limit(least_rank)
        versions = np.array([image["num_version"] for image in images])
        return least_rank, np.mean(versions), \
               np.min(versions), \
               np.max(versions), \
               np.median(versions), \
               np.std(versions)

    def query_version_sharing(self):
        pass

    def query_layer_top_size(self, least_share_count=100):
        """fetch layers information that have at least a given share count"""
        layers = self.layers.find({"share_count": {"$gt": least_share_count}})
        layer_count, total_count = layers.count(), self.query_layer_count()
        layer_ratio = layer_count / total_count
        total_size = sum([int(layer["size"]) for layer in layers]) // mb
        return least_share_count, layer_count, layer_ratio * 100, total_size

    def patch_layer_size(self, least_share_count=0, start_rank=0):
        layers = self.layers.find({"share_count": {"$gt": least_share_count}}) \
            .sort("share_count", -1) \
            .skip(start_rank)
        counter, batch_size = 0, 100
        layer_count, repo_digest_pairs = layers.count(), []
        for layer in layers:
            if "size" not in layer:
                repo_digest_pairs.append((layer["images"][0], layer["digest"]))
        while counter < layer_count:
            start = counter
            counter += batch_size
            if counter >= layer_count:
                end = layer_count
            else:
                end = counter
            batch = repo_digest_pairs[start:end]
            # fetch the sizes in batch
            sizes = hub.get_layer_size_batch(batch)
            # update the database entry; should change to in memory updates, however
            for pair, size in zip(batch, sizes):
                self.layers.update({"digest": pair[1]},
                                   {
                                       "$set": {"size": size}
                                   }
                                   )
                print("--> updated {}/{} layers.".format(counter, len(repo_digest_pairs)))

    def patch_invert_in_memory(self):
        """
        Invert the image-layer to layer-image and store in the layers collection; idempotent
        This version is 10000x faster than the disk version; if the dataset fits
        """
        self.images.create_index([("popularity", pymongo.DESCENDING)])
        images, layer_dict, image_counter = self.images.find().sort("popularity", -1), dict(), 0
        timestamp, timer = ts_gen(), Timer()
        for image in images:
            layers = image["tags"]["latest"]["layers"]
            image_name, popularity = image["name"], image["popularity"]
            for layer in layers:
                if layer not in layer_dict:
                    layer_dict[layer] = {"digest": layer,
                                         "timestamp": timestamp,
                                         "images": set(),
                                         "popularity": 0,
                                         "share_count": 0,
                                         }
                # update statistics
                layer_images = layer_dict[layer]["images"]
                if image_name not in layer_images:
                    layer_dict[layer]["popularity"] += popularity
                    layer_dict[layer]["share_count"] += 1
                    layer_images.add(image_name)
            image_counter += 1
            print("--> checked {} images.".format(image_counter))

        # mongodb does not encode set, translate to list first
        for digest, layer_doc in layer_dict.items():
            layer_doc["images"] = list(layer_doc["images"])
        self.layers.insert(layer_dict.values())
        print("--> {} layers writen to database.".format(self.layers.find().count()))

    def patch_official_images(self):
        pass

    def patch_image_depth(self):
        """Add how many layers in the image; idempotent"""
        images = self.images.find().sort("popularity", -1)
        counter = 0
        for image in images:
            layers = image["tags"]["latest"]["layers"]
            id = image["_id"]
            self.images.update({"_id": id},
                               {"$set": {"depth": len(layers)}},
                               )
            counter += 1
            print("--> {} images updated.".format(counter))

    def patch_layer_share_count(self):
        """Add the image share count; idempotent"""
        layers, counter = self.layers.find().sort("popularity", -1), 0
        for layer in layers:
            images = layer["images"]
            self.layers.update({"digest": layer},
                               {
                                   "$set": {"share_count": len(images)},
                               },
                               )
            counter += 1
            print("--> {} layers updated.".format(counter))

    def patch_invert(self, start_image=1, end_image=sys.maxsize):
        """Invert the image-layer to layer-image and store in the layers collection; idempotent"""
        images = self.images.find().sort("popularity", -1)
        image_counter, timestamp, timer = 0, ts_gen(), Timer()
        for image in images:
            if image_counter < start_image:
                image_counter += 1
                continue
            if image_counter >= end_image:
                return

            layers = image["tags"]["latest"]["layers"]
            popularity, image_name = image["popularity"], image["name"]
            for layer in layers:
                # using cursor.count can be 100x slower than what's used here
                record_count = self.layers.find({"digest": layer}).limit(1).count(with_limit_and_skip=True)
                if record_count > 1:
                    print("--> duplicate layer found, abort.")
                    sys.exit(1)
                elif record_count == 1:
                    self.layers.update({"digest": layer},
                                       {
                                           "$addToSet": {"images": image_name},
                                       },
                                       )
                else:
                    self.layers.update({"digest": layer},
                                       {"digest": layer,
                                        "timestamp": timestamp,
                                        "images": [image_name],
                                        },
                                       upsert=True)
                print("--> {} layers updated.".format(self.layers.count()))
            image_counter += 1
            print("--> {} images scanned.".format(image_counter))

    def patch_tags(self, least_rank=100, full_scan=False):
        """ patch image records with tag and digest info
        This method uses the async func by default; note that if the tag_only
        is enabled, the database may ends up in inconsistent state: some image
        tag records may not have been filled with digests. Enable full_scan to
        fix the missing digests.
        """
        images = self.images.find().sort("popularity", -1).limit(least_rank)
        # start the patching coroutine only when the image has not been tags
        # namely with only the "latest" tag, when tag_only is enabled, patch
        # without the digests. We are not using image["num_version"] here as
        # it may have not been created yet.
        futures = [self._patch_tags_func(image)
                   for image in images if len(image["tags"]) <= 1 or full_scan]

        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*futures))
        print("--> patched {} images.".format(least_rank))

    async def _patch_tags_func(self, image):
        """update the tags of a single image"""
        repo, image_tags = image["name"], image["tags"]
        patch_tags = await asyncio.coroutine(hub.get_tags)(image["name"])
        for tag in patch_tags:
            if tag == "latest":
                continue
            # mongodb doesn't work with key name with dot in it,
            # we need to replace it with something else: here every
            # dot will be replace with a comma -- distinguishable as
            # long as people don't tag containers with comma separated
            # phrases or words
            tag = tag.replace(".", ",")
            image_tags[tag] = {"layers": []}
        await asyncio.coroutine(self.images.update)({"name": repo},
                                                    {"$set": {"tags": image_tags,
                                                              "num_version": len(image_tags),
                                                              }}
                                                    )
        print("--> patched image {}:{}".format(repo, patch_tags))

    def patch_version_count(self):
        """patch the number of versions.
        This function should be use only when tags are updated without
        using patch_tags.
        """
        images = self.images.find().sort("popularity", -1)
        counter = 0
        for image in images:
            num_tags = len(image["tags"])
            id = image["_id"]
            self.images.update({"_id": id},
                               {"$set": {"num_version": num_tags}},
                               )
            counter += 1
            print("--> {} images updated.".format(counter))

    def check_unique_layers(self):
        images, layer_set, counter = self.images.find().sort("popularity", -1), set(), 0
        for image in images:
            layers = image["tags"]["latest"]["layers"]
            [layer_set.add(l) for l in layers]
            counter += 1
            print("--> checked {} images.".format(counter))
        print(len(layer_set))

    def check_image_uniqueness(self):
        map_func = "function () {emit(this.name, 1);}"
        reduce_func = "function (k, v) {return Array.sum(v)"
        self.images.map_reduce(map_func, reduce_func, "dup_images")
        pp.pprint(self.image_db.temp.find({"value": {"$gt": 1}}))

    def scoring_test(self, image):
        image = self.images.find_one({"name": image})
        image_name, digests = image["name"], image["tags"]["latest"]["layers"]
        layers, counter = self.layers.find({}), 0
        layer_dict, timer = {}, Timer()
        for layer in layers:
            layer_dict[layer["digest"]] = 1
        timer.start()
        print("--> start timing (total {}/{} layers).".format(len(digests), len(layer_dict)))
        for digest in digests:
            if digest in layer_dict:
                counter += 1
        timer.stop_and_report("layer matching (result: {}/{}).".format(counter, len(digests)))


def main():
    analyzer = Analyzer()

    analyzer.index()
    # analyzer.patch_layer_size_async()
    # print(analyzer.query_layer_top_size(100))
    # analyzer.patch_tags(least_rank=1000)
    # print("--> average version count is {}.".format(analyzer.query_image_versions(1000)))


if __name__ == "__main__":
    main()
