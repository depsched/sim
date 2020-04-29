#!/bin/bash

eval $(source ../../config/env.exp; aws ecr get-login --no-include-email --region $AWS_REGION)
