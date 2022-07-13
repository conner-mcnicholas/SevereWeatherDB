#!/bin/bash
WORKSPACE=workspace
docker run -it --rm \
    -v $(pwd)/vars:/${WORKSPACE}/vars \
    -v $(pwd)/scripts:/${WORKSPACE}/scripts \
    -w /${WORKSPACE} mcr.microsoft.com/azure-cli
