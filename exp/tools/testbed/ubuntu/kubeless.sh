#!/bin/bash

wget https://github.com/kubeless/kubeless/releases/download/v0.1.0/kubeless_linux-amd64.zip
unzip *.zip
rm *.zip
sudo mv ./bundles/kubeless_linux-amd64/kubeless /usr/local/bin/
rm -rf ./bundles

kubectl create ns kubeless
curl -sL https://github.com/kubeless/kubeless/releases/download/0.0.18/kubeless-0.0.18.yaml | kubectl create -f -
