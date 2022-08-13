#!/bin/bash
WORKSPACE=workspace
docker run -it --rm \
    -v $(pwd)/create_resources.sh:/${WORKSPACE}/create_resources.sh \
    -v $(pwd)/.secrets:/${WORKSPACE}/.secrets \
    -w /${WORKSPACE} mcr.microsoft.com/azure-cli
