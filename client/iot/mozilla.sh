#!/usr/bin/env bash

docker run \
    -d \
    --rm \
    -p 8080:8080 \
    -p 4443:4443 \
    -v /home/node/.mozilla-iot:/home/node/.mozilla-iot \
    --name webthings-gateway \
    mozillaiot/gateway:latest