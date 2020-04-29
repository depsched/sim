#!/bin/bash

REPO_LIST=$(cat max_image_set.csv | cut -d'/' -f 2)

for repo in $REPO_LIST; do
   aws ecr create-repository --repository-name $repo
done
