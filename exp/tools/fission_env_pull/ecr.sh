#!/bin/bash

# For IAM policy creation, check: http://docs.aws.amazon.com/AmazonECR/latest/userguide/ECR_IAM_user_policies.html

source ../../config/env.exp
aws ecr get-login --no-include-email --region $AWS_REGION

