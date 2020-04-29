#!/usr/bin/env python3
"""This scripts run docker pull for each image provided; parse the log
and write to the image database. An instrumented docker engine is used
to generate the log. The default log location is /tmp/.log."""
import glob
import json
import sys
from collections import defaultdict

import boto3

from .ecr import login_ecr, download_profiled_record, ECRImageDB, PROFILED_ECR_IMAGES, AWS_REGION
from .utils import cmd, cmd_ignore_error, dir_path

PULL_LOG = dir_path + "/__result__/docker_pull.log"
PULL_LOG_DIR = dir_path + "/__result__/__bak__/"
PULL_LOG_S3_BUCKET = "pull-logs"
RAW_PULL_LOGS = "/tmp/*_docker_pull.log"  # change this to actual log file


def pull_ecr_images(images=None, *,
                    skip_profiled=True,
                    ignore_error=True):
    """One by one pull images from ECR, given a list of images.

    The image/repository name must start with the ECR address.
    """
    ecr_address = login_ecr()

    def _prepare():
        """T'chapulla."""
        # obtain the log file;
        # clear all existing images;
        # and get this man a shield. ((x))
        cmd("docker pull alpine", quiet=True)
        log_file = sorted(glob.glob(RAW_PULL_LOGS), reverse=True)[0]

        # clear log and remove all images
        cmd("docker rmi $(docker images -aq)", quiet=True)
        cmd("rm {}".format(log_file))
        print("--> identified latest log file: {}.".format(log_file))
        print("--> start pulling..")
        return log_file

    def _patch_log(image, log_file):
        """
        add the image name after the log entries;
        replace the log name with the actual log name
        """
        with open(log_file, "a") as f: f.write(image + "\n")

    if not images:
        images = ECRImageDB().get_ecr_repositories()

    if skip_profiled:
        images = set(images)
        profiled_image = load_profiled_images()
        for image in profiled_image:
            images.remove(image)
        print("--> images to pull are:", images)

    log_file = _prepare()
    for count, image in enumerate(images):
        if not image.startswith(ecr_address):
            assert "/" not in image, "--> ill-formed image name {}.".format(image)
            image = ecr_address + "/" + image
        try:
            _patch_log(image, log_file)
            image_id = pull_image(image)
            remove_local_image(image_id)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            print("--> unable to pull image: {}.".format(image))
            if ignore_error:
                print("--> continue.")
            else:
                sys.exit(1)
        print("--> pulled {} images.".format(count))
    upload_pull_log_s3(log_file)


def pull_image(image, tag="latest"):
    """Given a image full name, pull the image and returns the image id."""
    try:
        # cmd("sudo docker pull {}:{}".format(image, tag))
        cmd("sudo docker pull {}".format(image))
        return cmd("sudo docker images {} -q".format(image), quiet=True).decode("ascii").rstrip()
    except KeyboardInterrupt:
        raise
    except:
        print("--> image missing on docker hub: {}.".format(image))
        raise Exception


def load_profiled_images():
    download_profiled_record()
    with open(PROFILED_ECR_IMAGES) as f:
        return json.load(f)


def remove_local_image(image_id):
    """Given a image full name, remove the image."""
    cmd_ignore_error("sudo docker rmi -f {}".format(image_id))


def upload_pull_log_s3(log_path=None, create_new_bucket=False):
    from .utils import ts_gen

    # upload the latest if no log is given
    if not log_path:
        log_path = sorted(glob.glob(RAW_PULL_LOGS), reverse=True)[0]

    log_name = log_path.split("/")[-1]

    # create backup
    if create_new_bucket:
        client = boto3.client('s3')
        client.create_bucket(
            ACL='private',
            Bucket=PULL_LOG_S3_BUCKET,
            CreateBucketConfiguration={
                'LocationConstraint': AWS_REGION,
            },
        )
    # upload to s3
    print("--> uploading pull log {} to S3..".format(log_name))
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(Filename=log_path,
                               Bucket=PULL_LOG_S3_BUCKET, Key=log_name + "_upload_at_" + ts_gen())
    # clear up
    print("--> done.")


def download_pull_log_s3(log_name=None):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(PULL_LOG_S3_BUCKET)
    if not log_name:
        log_names = [obj.key for obj in bucket.objects.all()]
        latest_log = sorted(log_names, reverse=True)[0]
        downloaded_file = PULL_LOG_DIR + latest_log + ".txt"
        bucket.download_file(latest_log, downloaded_file)
        # replace the current pull log
        cmd_ignore_error("rm {}".format(PULL_LOG))
        cmd("chmod 444 {}; cp -p {} {}".format(downloaded_file, downloaded_file, PULL_LOG))
    else:
        pass
    print("--> downloaded from S3, backed up, and updated the pull log.")


def clear_pull_log():
    cmd("rm /tmp/*_docker_pull.log")
    # open(PULL_LOG, "w").close()


def parse_pull_log():
    """Output a dict of images and a dict of layers."""
    images, layers = defaultdict(dict), defaultdict(dict)
    with open(PULL_LOG, "r") as f:
        image_name, image_record = None, None
        for log in f.readlines():
            log = log.split()
            if len(log) == 1:
                # keep the raw image name only
                image_name = log[0].split("/")[1]
                image_record = images[image_name]
                image_record["name"] = image_name
                image_record["size"] = 0
                image_record["pull_time"] = 0
                image_record["layers"] = set()
                continue

            digest = log[1]
            if digest not in layers:
                layer_record = layers[digest]
                layer_record["digest"] = digest
                layer_record["dl_time"] = 0
                layer_record["reg_time"] = 0
                layer_record["size"] = 0
                # below will be handled when saving to database
                layer_record["images"] = set()
                layer_record["popularity"] = 0
                layer_record["share_count"] = 0

            image_record = images[image_name]
            layer_record = layers[digest]
            image_record["layers"].add(digest)

            if log[4] == "registration":
                size = int(log[3])
                reg_time = int(log[6])
                image_record["size"] += size
                image_record["pull_time"] += reg_time
                layer_record["reg_time"] = reg_time
                layer_record["size"] = size
            elif log[4] == "download":
                dl_time = int(log[6])
                image_record["pull_time"] += dl_time
                layer_record["dl_time"] = dl_time
            else:
                print("--> pull log contains error.")
                sys.exit(1)
    return images, layers


def _unit_test():
    import pprint as pp
    pp.pprint(parse_pull_log()[0])


def main():
    pull_ecr_images()
    print("--> please use ecr.py to pull.")
    sys.exit(1)


if __name__ == "__main__":
    main()
