#!/bin/bash

ECR_ADDR=$(./ecr-cred.sh | grep -oP '(?<=https://).*')

IMAGE_SET="./max_image_set.csv"
ECR_IMAGE_LIST="./ecr_image_list.csv"

> $ECR_IMAGE_LIST

# pull, tag, and push each image to aws ecr;
for image in $(cat $IMAGE_SET); do
    sudo docker pull $image
    id=$(docker images $image -q)
    name=$(echo $image)
    echo $id $name

    docker tag $id $ECR_ADDR/$name
    docker push $ECR_ADDR/$name

    echo $ECR_ADDR/$name >> $ECR_IMAGE_LIST
done
