#!/bin/bash

ECR_URL=$(ecr.sh | grep -oP '(?<=https://).*')

docker build -t depsched .
docker tag depsched:latest $ECR_URL/depsched:latest
docker push $ECR_URL/depsched:latest


