#!/bin/bash

# Build depsched image and push to remote repo
source ../../../config/env.exp

cp build.sh $KUBE_ROOT_DIR
cp Dockerfile $KUBE_ROOT_DIR
cp env.exp $KUBE_ROOT_DIR
cp sample.dockerignore $KUBE_ROOT_DIR/.dockerignore

cd $KUBE_ROOT_DIR

# Build and push
bash ./build.sh

rm build.sh
rm Dockerfile
rm env.exp
rm .dockerignore


