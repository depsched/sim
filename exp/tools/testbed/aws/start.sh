#!/bin/bash

source env.exp

# default availability zone is zone 'a'

kops update cluster $NAME --yes


