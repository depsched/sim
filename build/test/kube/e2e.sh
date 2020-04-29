#!/usr/bin/env bash

source env.exp

cd ${KUBE_ROOT}

go run ./hack/e2e.go -v -up -down -kops `which kops` -kops-cluster ${NAME} -kops-state ${KOPS_STATE_STORE}  -kops-nodes=
${KUBE_NODE_COUNT} -deployment kops --kops-kubernetes-version https://storage.googleapis.com/kubernetes-release-dev/ci/$(curl  -SsL https://storage.googleapis.com/kubernetes-release-dev/ci/latest-green.txt)
