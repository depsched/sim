#!/bin/bash

# This script assumes ./fission.sh has been done
# install helm server on the current cluster

kubectl -n kube-system create sa tiller
kubectl create clusterrolebinding tiller --clusterrole cluster-admin --serviceaccount=kube-system:tiller
helm init --service-account tiller

# install fission
helm install --namespace fission --set serviceType=NodePort https://github.com/fission/fission/releases/download/v0.2.1/fission-all-v0.2.1.tgz


