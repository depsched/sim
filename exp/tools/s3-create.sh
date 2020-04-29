#!/bin/bash

STORE_NAME=$1
AWS_REGION=$2

aws s3api create-bucket \
        --bucket $STORE_NAME \
        --region $AWS_REGION \
        --create-bucket-configuration LocationConstraint=$AWS_REGION

aws s3api put-bucket-versioning --bucket $STORE_NAME --versioning-configuration Status=Enabled
