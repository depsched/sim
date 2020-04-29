#!/bin/bash

# if kubectl not installed: ./kubectl.sh
command -v kubectl >/dev/null 2>&1 || { ./kubectl.sh; }
kubectl version || { echo 'kubectl failed, is it correctly installed?' ; exit 1; }

# install helm cli

curl -LO https://storage.googleapis.com/kubernetes-helm/helm-v2.6.1-linux-amd64.tar.gz
tar xzf helm-v2.6.1-linux-amd64.tar.gz
sudo mv linux-amd64/helm /usr/local/bin

# install helm server
kubectl -n kube-system create sa tiller
kubectl create clusterrolebinding tiller --clusterrole cluster-admin --serviceaccount=kube-system:tiller
helm init --service-account tiller

# install fission (with minikube)
helm install --namespace fission --set serviceType=NodePort https://github.com/fission/fission/releases/download/v0.2.1/fission-all-v0.2.1.tgz

# install fission cli
curl -Lo fission https://github.com/fission/fission/releases/download/v0.2.1/fission-cli-linux && chmod +x fission && sudo mv fission /usr/local/bin/

# set env
export FISSION_URL=http://$(minikube ip):31313
export FISSION_ROUTER=$(minikube ip):31314
