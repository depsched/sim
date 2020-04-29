#!/usr/bin/env bash

STORE_NAME=$1
DIR=$2
TARGET_DIR=$3

#aws s3 sync --acl public-read ${DIR} s3://${STORE_NAME}/${TARGET_DIR}
aws s3 cp ${DIR} s3://${STORE_NAME}/
