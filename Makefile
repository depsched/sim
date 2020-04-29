SHELL := /bin/bash
THIS_VER := v0.1.0

# Extra environment variables
.EXPORT_ALL_VARIABLES:
OUT_DIR ?= _output
BIN_DIR := $(OUT_DIR)/bin
RELEASE_DIR := $(OUT_DIR)/$(THIS_VER)
# BINARY := 
# CMD_ENTRY := cmd.go
# IMAGE := 

VERBOSE ?= 1

clean: @rm -r $(OUT_DIR) || true

# create test clusters
.PHONY: ec2, delete-ec2

ec2:
	REGION=$(REGION); NAME=$(CLUSTER_NAME) python3 -m build.kube.cluster up

show-ec2:
	bash ./build/get_clusters.sh

delete-ec2:
	python3 -m build.kube.gen_spec $(CLUSTERID); python3 -m build.kube.cluster down

