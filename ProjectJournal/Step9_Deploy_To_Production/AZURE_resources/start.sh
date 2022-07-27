#!/bin/bash
WORKSPACE=workspace
docker run -it --rm \
    -v $(pwd)/create_resources.sh:/${WORKSPACE}/create_resources.sh \
    -v $(pwd)/.secrets:/${WORKSPACE}/.secrets \
    -v $(pwd)/DATAFACTORY_pipelines:/${WORKSPACE}/DATAFACTORY_pipelines \
    -w /${WORKSPACE} mcr.microsoft.com/azure-cli
