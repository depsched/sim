#!/usr/bin/env python3
"""Image database with functions to fetch and push images to EC2
container registry. It also extends basic image database with backup
capability to s3."""

import asyncio
import pprint as pp
import re
import sys
from collections import defaultdict

import boto3
import pymongo

from .imagedb import BasicImageDB, IMAGE_DB_PATH
from .utils import cmd, gb, mb, time_func, dir_path

AWS_REGION = "us-west-1"
IMAGE_DB_S3_BUCKET = "image-db-store"
LOG_S3_BUCKET = "depsched-logs"

PROFILED_ECR_IMAGES = dir_path + "/__result__/ecr_images_profiled.json"
ECR_IMAGES = dir_path + "/__result__/ecr_images.json"

ASSUMED_COMPRESS_RATIO = 2


def login_ecr():
    """Login from local docker client to the ec2 repository."""
    global ECR_REGISTRY_ADDRESS, IS_LOGIN
    if "IS_LOGIN" in globals() and globals()["IS_LOGIN"]:
        return ECR_REGISTRY_ADDRESS

    def _ecr_credentials():
        out = cmd("aws ecr get-login --no-include-email --region {}".format(AWS_REGION), quiet=True)
        return out.decode("ascii").rstrip()

    ECR_CREDENTIALS = _ecr_credentials()
    ECR_REGISTRY_ADDRESS = re.search("(?<=https://).*", ECR_CREDENTIALS).group(0)
    cmd(ECR_CREDENTIALS);
    IS_LOGIN = True
    return ECR_REGISTRY_ADDRESS


def push_to_ecr(image_name, image_local_id):
    """Update the image repository, push, and write to the ECR image list."""
    ecr_repo = "{}/{}".format(ECR_REGISTRY_ADDRESS, image_name)
    cmd("docker tag {image_id} {ecr_repo}".format(image_id=image_local_id,
                                                  ecr_repo=ecr_repo))
    cmd("docker push {}".format(ecr_repo))


def backup_to_s3(create_new_bucket=False):
    from .utils import ts_gen
    # create backup
    print("--> making backup..")
    backup_file = "image_db_bak_{}.tar.gz".format(ts_gen())
    cmd("tar czf {} {}".format(backup_file, IMAGE_DB_PATH))
    if create_new_bucket:
        client = boto3.client('s3')
        client.create_bucket(
            ACL='private',
            Bucket=IMAGE_DB_S3_BUCKET,
            CreateBucketConfiguration={
                'LocationConstraint': AWS_REGION,
            },
        )
    # upload to s3
    print("--> uploading..")
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(Filename=backup_file,
                               Bucket=IMAGE_DB_S3_BUCKET, Key=backup_file)
    # clear up
    cmd("rm {}".format(backup_file))
    print("--> done.")


def upload_profiled_record():
    # create backup
    print("--> making backup for profiled record..")
    backup_file = PROFILED_ECR_IMAGES
    cmd("chmod 666 {}".format(backup_file))
    # upload to s3
    print("--> uploading..")
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(Filename=backup_file,
                               Bucket=LOG_S3_BUCKET, Key=backup_file)
    print("--> done.")


def download_profiled_record():
    # create backup
    print("--> downloading backup for profiled record..")
    backup_file = PROFILED_ECR_IMAGES
    s3 = boto3.resource('s3')
    s3.meta.client.download_file(LOG_S3_BUCKET, backup_file, backup_file)
    cmd("chmod 666 {}".format(backup_file))
    print("--> done.")


class ECRImageDB(BasicImageDB):
    """Manage both access to EC2 repository and local database.

    The methods assume the repository name on ECR is equivalent to the image
    name, i.e., they consider the "latest" tag of an image only.
    """

    def __init__(self):
        super().__init__()

    def __getattr__(self, name):
        if name == "estimator":
            self.estimator = self.make_estimator()
            return self.estimator
        if name == "ecr_address":
            self.ecr_address = login_ecr()
            return self.ecr_address
        if name == "ecr_client":
            self.ecr_address = login_ecr()
            self.ecr_client = boto3.client('ecr')
            return self.ecr_client
        if name == "ecr_images":
            self.ecr_images = self.image_db.ecr_images
            return self.ecr_images
        if name == "ecr_layers":
            self.ecr_layers = self.image_db.ecr_layers
            return self.ecr_layers
        if name == "pcr_images":
            self.pcr_images = self.image_db.pcr_images
            return self.pcr_images
        if name == "pcr_layers":
            self.pcr_layers = self.image_db.pcr_layers
            return self.pcr_layers
        else:
            return super().__getattr__(name)

    @time_func
    def save_ecr_images(self, clear_all=False, update=False):
        """Consolidate official images and layers info from file with the top images
        information from the database. The results are stored in their own collections.
        This should be run after you collect the pull time data.

        Any entry in these collections will have their pull time data ready.
        #TODO: add different profiling instance types.
        """
        if clear_all:
            print("--> clear ecr_images; {} images removed"
                  .format(self.ecr_images.delete_many({}).deleted_count))
            print("--> clear ecr_layers; {} layers removed"
                  .format(self.ecr_layers.delete_many({}).deleted_count))

        from .pull import parse_pull_log
        from .imagedb import load_official_image_stats

        profiled_ecr_images, profiled_ecr_layers = parse_pull_log()
        # load the images and layers that are already on the database
        # TODO: load as dict for calculating average values
        print("--> loading existing images and layers..")
        ecr_images = {entry["name"] for entry in self.ecr_images.find()}
        ecr_layers = {entry["digest"]: entry for entry in self.ecr_layers.find()}

        # patch the popularity
        print("--> preparing new entries to add..")
        official_image_stats = load_official_image_stats()
        images_to_remove = []
        for name, entry in profiled_ecr_images.items():
            # skip if the image has been previously profiled
            if name in ecr_images and not update:
                images_to_remove.append(name)
                continue
            if name in official_image_stats:
                entry["popularity"] = official_image_stats[name]["popularity"]
            else:
                # TODO: slow operation, should change after fix repository naming issues
                try:
                    db_entry = self.images.find_one({"name": "{}"
                                                    .format(name.replace("-", "/", 1))})
                    if db_entry is None:
                        entry["popularity"] = next(self.images.find({"name": {"$regex": ".*\/{}"
                                                                    .format(name)}})
                                                   .sort("popularity", -1)
                                                   .limit(1))["popularity"]
                    else:
                        entry["popularity"] = db_entry["popularity"]
                except:
                    # if the image does not exist in the database, skip it
                    print("--> skip image {}.".format(name))
                    images_to_remove.append(name)
                    continue

            # make the set a list, compatible with mongodb
            entry["layers"] = list(entry["layers"])
            entry["depth"] = len(entry["layers"])

            # process the layers in this image
            try:
                for digest in entry["layers"]:
                    assert digest in profiled_ecr_layers
                    profiled_ecr_layers[digest]["popularity"] += entry["popularity"]
                    profiled_ecr_layers[digest]["images"].add(name)
                    profiled_ecr_layers[digest]["share_count"] += 1
            except AssertionError:
                print("--> trace contains error.")
                sys.exit(1)

        for name in images_to_remove:
            del profiled_ecr_images[name]

        layers_to_remove = []
        # process and make ecr_layers compatible with mongodb
        for digest, entry in profiled_ecr_layers.items():
            # skip if the layer has been previously profiled
            if len(entry["images"]) == 0:
                layers_to_remove.append(digest)
            else:
                entry["images"] = list(entry["images"])
                entry["pull_time"] = entry["reg_time"] + entry["dl_time"]
                if digest in ecr_layers:
                    # just update the existing layer's image list
                    new_images = list(set(ecr_layers[digest]["images"] + entry["images"]))
                    self.ecr_layers.update({"digest": digest}, {"$set": {"images": new_images}})
                    layers_to_remove.append(digest)

        for digest in layers_to_remove:
            del profiled_ecr_layers[digest]

        # dump to db
        print("--> inserting to database..")
        if len(profiled_ecr_images) > 0:
            self.ecr_images.insert_many(profiled_ecr_images.values())
        if len(profiled_ecr_layers) > 0:
            self.ecr_layers.insert_many(profiled_ecr_layers.values())
        print("--> inserted {} images and {} layers".format(len(profiled_ecr_images),
                                                            len(profiled_ecr_layers)))

        # build index
        print("--> indexing..")
        self.ecr_images.create_index([("popularity", pymongo.DESCENDING),
                                      ("name", pymongo.TEXT)])
        self.ecr_layers.create_index([("popularity", pymongo.DESCENDING),
                                      ("digest", pymongo.TEXT)])

        # export to file; make sure every time you update the database,
        # this file is exported correctly, if you want to skip profiled
        # images when run "pull"
        self.export_profiled_images()
        upload_profiled_record()
        print("--> done.")

    @time_func
    def check_db_healthy(self, purge=True, fix=True, pcr=True):
        """Supports check on ECR images and layers only."""
        # check images
        images, layers = set(), set()
        layers_in_images = set()
        images_own_layers = set()
        ecr_images = self.pcr_images if pcr else self.ecr_images
        ecr_layers = self.pcr_layers if pcr else self.ecr_layers
        layer_info = {entry["digest"]: entry for entry in ecr_layers.find()}
        for entry in ecr_images.find():
            name = entry["name"]
            # uniqueness check
            assert name not in images, "--> duplicated image found: {}".format(name)
            images.add(name)
            # make sure fields are there
            for field in ["pull_time", "size", "popularity", "layers"]:
                assert field in entry, "--> missing field {} in image {}.".format(field, name)
            # make sure the layers are not empty
            try:
                assert len(entry["layers"]) > 0
            except:
                print(entry)
                print("--> empty layers on image {}.".format(name))
                if purge:
                    ecr_images.delete_one({"name": name})
                    print("--> purged.")
                else:
                    raise Exception
            # make sure the sum of layer size agrees with the image size
            # make sure any layers listed in the image, the image is also recorded in the layer's images record
            image_size = 0
            for digest in entry["layers"]:
                image_size += layer_info[digest]["size"]
                layers_in_images.add(digest)
                try:
                    assert name in layer_info[digest]["images"]
                except:
                    print("--> inconsistent layer images record; image: {}, layer: {}.".format(name, digest))
                    if fix:
                        print("--> inserting image {} to layer {}".format(name, digest))
                        layer_info[digest]["images"].append(name)
                        ecr_layers.update({"digest": digest}, {"$set": {"images": layer_info[digest]["images"]}})

            assert image_size == entry["size"], \
                "--> inconsistent image size; image: {} layer-sum: {}.".format(entry["size"], image_size)

        for entry in ecr_layers.find():
            digest = entry["digest"]
            # uniqueness check
            assert digest not in layers, "--> duplicated layer found: {}".format(digest)
            layers.add(digest)
            # make sure fields are there
            for field in ["pull_time", "size", "popularity", "images"]:
                assert field in entry, "--> missing field {} in layer {}.".format(field, digest)
            # make sure images are not empty
            assert len(entry["images"]) > 0, "--> unused layer {}.".format(digest)
            for name in entry["images"]:
                images_own_layers.add(name)

        # make sure any layers mentioned in the image entry exists in the db
        # and vice versa.
        for digest in layers_in_images:
            assert digest in layers

        for name in images_own_layers:
            assert name in images

        if pcr:
            print("--> pcr images checked; start checking ecr images..")
            self.check_db_healthy(pcr=False)
        print("--> all checks passed; I think database is healthy (pcr: {}).".format(pcr))

    def clear_ecr_images(self):
        print("--> clear ecr_images; {} images removed"
              .format(self.ecr_images.delete_many({}).deleted_count))
        print("--> clear ecr_layers; {} layers removed"
              .format(self.ecr_layers.delete_many({}).deleted_count))

    def export_profiled_images(self):
        import json
        dump_dict = {}
        with open(PROFILED_ECR_IMAGES, "w") as f:
            for entry in self.ecr_images.find():
                dump_dict[entry["name"]] = 1
            f.write(json.dumps(dump_dict))
        print("--> exported to file {}.".format(PROFILED_ECR_IMAGES))

    def export_ecr_images(self):
        assert ECR_REGISTRY_ADDRESS != "", "--> check if you have logged on ECR."
        counter = 0
        with open(ECR_IMAGES, "w") as f:
            for image in self.get_ecr_repositories():
                counter += 1
                f.write(ECR_REGISTRY_ADDRESS + "/" + image + "\n")
        print("--> done; exported {} images.".format(counter))

    def get_ecr_image_stats(self, readable=False, pcr=True):
        """Obtain some statistics from the ECR images and layers."""
        # TODO: use mongodb aggregation
        import numpy as np
        ecr_images, ecr_layers = self.ecr_images.find(), self.ecr_layers.find()
        image_pull_time, image_size, image_depth = [], [], []
        layer_pull_time, layer_dl_time, \
        layer_reg_time, layer_size, \
        layer_share_count = [], [], [], [], []
        images_checked, layers_checked = set(), set()
        for entry in ecr_images:
            images_checked.add(entry["name"])
            image_pull_time.append(entry["pull_time"])
            image_size.append(entry["size"])
            image_depth.append(entry["depth"])

        for entry in ecr_layers:
            layers_checked.add(entry["digest"])
            layer_pull_time.append(entry["pull_time"])
            layer_dl_time.append(entry["dl_time"])
            layer_reg_time.append(entry["reg_time"])
            layer_size.append(entry["size"])

        image_count = ecr_images.count()
        layer_count = ecr_layers.count()
        if pcr:
            pcr_images, pcr_layers = self.pcr_images, self.pcr_layers
            for entry in pcr_images.find():
                if entry["name"] not in images_checked:
                    image_pull_time.append(entry["pull_time"])
                    image_size.append(entry["size"])
                    image_depth.append(len(entry["layers"]))

            for entry in pcr_layers.find():
                if entry["digest"] not in layers_checked:
                    layer_pull_time.append(entry["pull_time"])
                    layer_dl_time.append(entry["dl_time"])
                    layer_reg_time.append(entry["reg_time"])
                    layer_size.append(entry["size"])
            image_count += pcr_images.count()
            layer_count += pcr_layers.count()

        return {"image_count": image_count,
                "total_image_size": "{}gb".format(sum(image_size) // gb) if readable else sum(image_size),
                "avg_image_size": "{}mb".format(np.mean(image_size) // mb) if readable else np.mean(image_size),
                "avg_image_depth": np.mean(image_depth),
                "layer_count": layer_count,
                "total_layer_size": "{}gb".format(sum(layer_size) // gb) if readable else sum(layer_size),
                "mean_image_pull_time": np.mean(image_pull_time),
                "mean_layer_pull_time": np.mean(layer_pull_time),
                }

    def get_ecr_images_total_size(self):
        """Get the size of the *compressed* images in GB."""
        repositories = self.get_ecr_repositories()
        total_size = 0
        futures = [self._get_ecr_image_size(repo) for repo in repositories]
        loop = asyncio.get_event_loop()
        print("--> collecting the size info..")
        results = loop.run_until_complete(asyncio.gather(*futures))
        total_size += sum(results)
        return len(futures), total_size / gb

    def get_ecr_repositories(self):
        """Return a list of images currently on ECR."""
        print("--> obtaining a list of images on ECR..")
        response = self.ecr_client.describe_repositories()
        next_token = response["nextToken"]
        repositories = [repo["repositoryName"]
                        for repo in response["repositories"]]
        while True:
            response = self.ecr_client.describe_repositories(nextToken=next_token)
            repositories.extend([repo["repositoryName"]
                                 for repo in response["repositories"]])
            if "nextToken" not in response:
                break
            else:
                next_token = response["nextToken"]
        return iter(repositories)

    async def _get_ecr_image_size(self, repository):
        """Obtain the size of the first 100 images in the repository."""
        response = await asyncio.coroutine(self.ecr_client.describe_images)(repositoryName=repository)
        try:
            image_size_bytes = sum([image["imageSizeInBytes"] for image in response["imageDetails"]])
        except:
            image_size_bytes = 0
        return image_size_bytes

    def create_ecr_repository(self, name):
        response = ""
        try:
            response = self.ecr_client.create_repository(
                repositoryName=name,
            )
            print("--> created repository: {}.".format(name))
            pp.pprint(response)
        except:
            pp.pprint(response)
            print("--> unable to create repository: {}"
                  "; move on.".format(name))

    def delete_ecr_repository(self, name):
        try:
            self.ecr_client.delete_repository(
                repositoryName=name,
            )
        except:
            print("--> unable to delete repository: {}"
                  "; move on.".format(name))

    def delete_all_repositories(self):
        # remove all ECR repositories
        for name in self.get_ecr_repositories():
            self.delete_ecr_repository(name)

        # remove all local databases
        self.ecr_images.delete_many({})
        self.ecr_layers.delete_many({})

    def rename_repositories(self, rank=10000):
        """Use the concatenated name (repo-image) to replace the image-only name.
        ECR does not support renaming currently."""
        # from .imagedb import load_official_images
        # self.connect_db()
        # # load official and top images
        # official_images = load_official_images()
        # top_images = {entry["name"] for entry in self.images.find().sort("popularity", -1).limit(rank)}
        # # load profiled images to rename
        # profiled_images = {entry["name"] for entry in self.ecr_images.find()}
        # # rename the official images
        # for name in official_images:
        #     full_name = "library-" + name
        pass

    def clone_images_to_ecr(self, images):
        """Clone images from docker store to ECR.

            Some operations can be done using boto3 while the others, such as image push,
            should be done using docker.
            """
        from .pull import pull_image, remove_local_image
        # we consider the latest image of the repository only
        # whose name is equivalent to the repository name since
        # the "latest" tag is implicit
        ecr_images = set(self.get_ecr_repositories())
        prev_length = len(ecr_images)
        for image in images:
            # though ECR supports nested naming, we
            # flatten it out to avoid docker client
            # issues
            image_name = image.replace("/", "-")
            # skip if already exists
            if image_name in ecr_images:
                print("--> skipped; already on ecr: {}.".format(image_name))
                continue

            # pull, push, and clean
            print("--> processing image {}..".format(image_name))
            try:
                image_id = pull_image(image)
                self.create_ecr_repository(image_name)
                push_to_ecr(image_name, image_id)
                remove_local_image(image_id)
                ecr_images.add(image_name)
                print("--> done.")
            except KeyboardInterrupt:
                self.delete_ecr_repository(image_name)
                print("--> push aborted.")
            except Exception as e:
                self.delete_ecr_repository(image_name)
                print(e, "\n--> skipped {} and continue.".format(image_name))

        print("--> done; {} images added.".format(len(ecr_images) -
                                                  prev_length))

    def clone_top_images_to_ecr(self, rank=2000, official=False):
        from .imagedb import load_official_images
        print("--> reading in profiled images..")
        self.connect_db()
        profiled_images = {entry["name"] for entry in self.ecr_images.find()}

        # skipped the profiled images; the images on ECR will be skipped later
        appeared = set()

        def profiled(image_name):
            """Only the first occurrence is seen as profiled. This is to fix
            the ECR repository naming problem with existing repositories."""
            if image_name in profiled_images and image_name not in appeared:
                appeared.add(image_name)
                return True
            else:
                return False

        print("--> getting a list of images to clone..")
        images = self.get_top_images(rank=rank) if not official \
            else self.get_top_images(rank=rank).extend(load_official_images())
        images = [image for image in images if not profiled(image.split("/")[1])]

        self.clone_images_to_ecr(images)

    def eval_estimator(self, held_out=100, eval_once=False):
        import numpy as np, pprint as pp
        from random import shuffle
        ecr_layers = list(self.ecr_layers.find())
        shuffle(ecr_layers)

        def cross_validation(k=None):
            print("--> set k to {}".format(k))
            dl_errors, reg_errors, all_errors = [], [], []
            for start in range(0, len(ecr_layers), held_out):
                print("--> at {}/{}".format(start + held_out, len(ecr_layers) - held_out))
                test_set = ecr_layers[start:start + held_out]
                training_set = ecr_layers[:start] + ecr_layers[(start + held_out):]
                if k is None:
                    estimator = self.make_estimator(layers=training_set)
                else:
                    estimator = self.make_estimator(layers=training_set, k=k, quiet=True)
                # evaluation
                avg_error_dl, avg_error_reg, avg_error_all = [], [], []
                size_weights, dl_weights, reg_weights, all_weights = [], [], [], []
                for entry in test_set:
                    size, real_dl, real_reg = entry["size"], entry["dl_time"], entry["reg_time"]
                    estimate_dl, estimate_reg = estimator(size)
                    # weighted error with layer size as the weight
                    size_weights.append(size)
                    dl_weights.append(real_dl)
                    reg_weights.append(real_reg)
                    all_weights.append(real_reg + real_dl)
                    avg_error_dl.append(abs(estimate_dl - real_dl) / (real_dl + 1))
                    avg_error_reg.append(abs(estimate_reg - real_reg) / (real_reg + 1))
                    avg_error_all.append(abs(estimate_reg - real_reg
                                             + estimate_dl - real_dl) / (real_reg + real_dl + 1))
                if eval_once:
                    pp.pprint((np.average(avg_error_dl, weights=dl_weights),
                               np.average(avg_error_reg, weights=reg_weights),
                               np.average(avg_error_all, weights=all_weights)))
                    break
                dl_errors.append(np.average(avg_error_dl, weights=dl_weights))
                reg_errors.append(np.average(avg_error_reg, weights=reg_weights))
                all_errors.append(np.average(avg_error_all, weights=all_weights))
            return np.average(dl_errors), np.average(reg_errors), np.average(all_errors)

        # print(cross_validation(k=1))
        print(cross_validation(k=15))

    def make_estimator(self, k=10, layers=None, quiet=False):
        """Generate an estimator function for the download time and reg. time
        for a layer given its size."""
        if not quiet:
            print("--> preparing layer pull time estimator..")

        import bisect
        import numpy as np
        layer_dl_time_map = defaultdict(list)
        layer_reg_time_map = defaultdict(list)
        ecr_layers = list(self.ecr_layers.find()) if layers is None else layers

        for entry in ecr_layers:
            size = entry["size"]
            layer_dl_time_map[size].append(entry["dl_time"])
            layer_reg_time_map[size].append(entry["reg_time"])

        layer_size_list = sorted(layer_dl_time_map.keys())

        def knn(index, data_map):
            """Returns an array of neighboring values."""
            neighbors = data_map[layer_size_list[index]]
            leftmost, rightmost = 0, len(layer_size_list) - 1
            pivot_right = pivot_left = index
            while len(neighbors) < k:
                progress = False
                # probe right
                if pivot_right < rightmost:
                    for data in data_map[layer_size_list[pivot_right + 1]]:
                        neighbors.append(data)
                    pivot_right += 1
                    progress = True
                if len(neighbors) >= k:
                    break
                # probe left
                if pivot_left > leftmost:
                    for data in data_map[layer_size_list[pivot_left - 1]]:
                        neighbors.append(data)
                    pivot_left -= 1
                    progress = True
                if not progress:
                    break
            return neighbors

        def estimator(size):
            location = bisect.bisect_left(layer_size_list, size) - 1
            if location < 0:
                location = 0
            return int(np.average(knn(location, layer_dl_time_map))), \
                   int(np.average(knn(location, layer_reg_time_map)))

        if not quiet:
            print("--> done.")
        return estimator

    def pcr(self, *, target=58000, skip=2000, clear_all=True):
        """Have you heard of PCR for DNA in Biology? Let's apply the idea here
        for profiling images and layers without profiling."""
        from .hub import get_layer_size
        print("--> doing pcr, targeting top {} images.".format(target))
        estimator = self.estimator
        self.pcr_images = self.image_db.pcr_images
        self.pcr_layers = self.image_db.pcr_layers
        pcr_images, pcr_layers = defaultdict(dict), defaultdict(dict)
        profiled_images = {entry["name"] for entry in self.ecr_images.find()}
        profiled_layers = {entry["digest"]: entry for entry in self.ecr_layers.find()}

        print("--> loading layer entries..")
        layer_size_map = dict()
        counter = 0
        for entry in self.layers.find():
            if counter % 10000 == 0:
                print("--> at entry: {}.".format(counter))
            counter += 1
            digest = entry["digest"]
            if "size" in entry:
                layer_size_map[digest] = int(entry["size"])
        print("--> done.")

        counter = 0
        # prepare the image record
        for entry in self.images.find().sort("popularity", -1).limit(target):
            name = entry["name"]
            if name in profiled_images or counter < skip:
                print("--> already profiled: {}.    ".format(name))
                counter += 1
                continue
            else:
                pcr_images[name] = {"popularity": entry["popularity"],
                                    "name": entry["name"],
                                    "layers": entry["tags"]["latest"]["layers"],
                                    "size": 0,
                                    "pull_time": 0,
                                    "depth": len(entry["tags"]["latest"]["layers"]),
                                    }

        images_to_remove = set()
        counter = 1
        # prepare the layer record # TODO: use batch dump to the database
        for name, image in pcr_images.items():
            print("--> processed {}/{} images.".format(counter, len(pcr_images)))
            counter += 1
            layers = image["layers"]
            popularity = image["popularity"]
            for digest in layers:
                if digest in pcr_layers:
                    entry = pcr_layers[digest]
                    entry["images"].append(name)
                    entry["popularity"] += popularity
                    entry["share_count"] += 1
                    pcr_images[name]["size"] += entry["size"]
                    pcr_images[name]["pull_time"] += entry["pull_time"]
                    continue
                if digest in profiled_layers:
                    entry = profiled_layers[digest]
                    size = entry["size"]
                    dl_time, reg_time = entry["dl_time"], entry["reg_time"]
                elif digest in layer_size_map:
                    size = layer_size_map[digest]
                    size *= ASSUMED_COMPRESS_RATIO
                    dl_time, reg_time = estimator(size)
                else:
                    print("--> patching remotely..")
                    repo = name
                    try:
                        size = int(get_layer_size(repo, digest))
                        size *= ASSUMED_COMPRESS_RATIO
                        dl_time, reg_time = estimator(size)
                    except:
                        print("--> unable to patch layer size, skip the image.")
                        images_to_remove.add(name)
                        continue
                pcr_layers[digest] = {"digest": digest,
                                      "dl_time": dl_time,
                                      "reg_time": reg_time,
                                      "size": size,
                                      "images": [name],
                                      "popularity": popularity,
                                      "share_count": 1,
                                      "pull_time": dl_time + reg_time
                                      }
                pcr_images[name]["size"] += pcr_layers[digest]["size"]
                pcr_images[name]["pull_time"] += pcr_layers[digest]["pull_time"]

        if clear_all:
            print("--> clear pcr_images; {} images removed"
                  .format(self.pcr_images.delete_many({}).deleted_count))
            print("--> clear pcr_layers; {} layers removed"
                  .format(self.pcr_layers.delete_many({}).deleted_count))

        # dump to db
        print("--> inserting to database..")
        if len(pcr_images) > 0:
            self.pcr_images.insert_many(pcr_images.values())
        if len(pcr_layers) > 0:
            self.pcr_layers.insert_many(pcr_layers.values())
        print("--> inserted {} images and {} layers".format(len(pcr_images),
                                                            len(pcr_layers)))
        # build index
        print("--> indexing..")
        self.pcr_images.create_index([("popularity", pymongo.DESCENDING),
                                      ("name", pymongo.TEXT)])
        self.pcr_layers.create_index([("popularity", pymongo.DESCENDING),
                                      ("digest", pymongo.TEXT)])

    def pcr_fastpass(self):
        """Only renews the existing entries with new estimates."""
        pass


def _unit_test():
    image_db = ECRImageDB()
    print(image_db.get_ecr_images_total_size())


def main():
    image_db = ECRImageDB()

    def list_ecr_images():
        images = list(image_db.get_ecr_repositories())
        print(images, len(images))

    from .pull import pull_ecr_images, upload_pull_log_s3, download_pull_log_s3
    from .utils import main_with_cmds

    cmds = {
        "stat": lambda: pp.pprint(image_db.get_ecr_image_stats(readable=True)),
        "size": lambda: print(image_db.get_ecr_images_total_size()),
        "clone": lambda: image_db.clone_top_images_to_ecr(rank=10000),
        "pcr": image_db.pcr,
        "pcr_fastpass": image_db.pcr_fastpass,
        "save": image_db.save_ecr_images,
        "start": image_db.start_db_daemon,
        "check": image_db.check_db_healthy,
        "clear": image_db.clear_ecr_images,
        "export": image_db.export_profiled_images,
        "rename": image_db.rename_repositories,
        "estimate": lambda: print(image_db.make_estimator()(1000 * mb)),
        "estimate_eval": lambda: image_db.eval_estimator(),
        "delete_all": image_db.delete_all_repositories,
        "backup": backup_to_s3,
        "pull": pull_ecr_images,
        "list": list_ecr_images,
        "upload_log": upload_pull_log_s3,
        "download_log": download_pull_log_s3,
        "upload_record": upload_profiled_record,
        "download_record": download_profiled_record,
        "test": _unit_test,
    }
    main_with_cmds(cmds)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit('\nInterrupted')
