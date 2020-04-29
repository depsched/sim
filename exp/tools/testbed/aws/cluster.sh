#!/bin/bash

source env.exp

# default availability zone is zone 'a'

kops create cluster --zones ${AWS_REGION}a $NAME


