#!/bin/bash

ECR_ADDR=$(./ecr-cred.sh | grep -oP '(?<=https://).*')

IMAGE="buildpack-deps"

# pull, tag, and push each image to aws ecr;
for image in $IMAGE; do
    sudo docker pull $image
    id=$(docker images $image -q)
    name=$(echo $image)
    echo $id $name

    docker tag $id $ECR_ADDR/$name
    docker push $ECR_ADDR/$name
done
