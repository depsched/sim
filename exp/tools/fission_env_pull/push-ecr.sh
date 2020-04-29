#!/bin/bash

ECR_ADDR=$(./ecr.sh | grep -oP '(?<=https://).*')

> env_ecr.csv

# pull, tag, and push each image to aws ecr;
for image in $(cat runtime_env.csv); do
    sudo docker pull $image
    id=$(docker images $image -q)
    name=$(echo $image | cut -d'/' -f 2)

    docker tag $id $ECR_ADDR/$name
    docker push $ECR_ADDR/$name

    echo $ECR_ADDR/$name >> env_ecr.csv
done
