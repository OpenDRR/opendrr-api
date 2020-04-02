#!/bin/bash -m
# -*- coding: utf-8 -*-

ROOT=$( pwd )

while getopts "c" opt; do
  case ${opt} in
    c)
      clean=1
      ;;
  esac
done

# Stop any running process
lsof -ti:5000 | xargs kill

if [ ! -d "$ROOT/venv-pygeoapi" ]; then
    python3 -m venv "venv-pygeoapi"
fi
source "$ROOT/venv-pygeoapi/bin/activate"

cd venv-pygeoapi
. bin/activate

# clean up previous cloned repo
if [ "${clean}" ]; then
    rm -r -f pygeoapi
fi

if [ ! -d "pygeoapi" ]; then

    git clone https://github.com/geopython/pygeoapi.git
    cd pygeoapi
    pip install -r requirements.txt
    pip install -r requirements-dev.txt
    pip install -r requirements-provider.txt
    # install starlette requirements accordingly from requirements-starlette.txt
    pip install -e .
    # create local configuration
    cp $ROOT/configuration/local.config.yml local.config.yml
    export PYGEOAPI_CONFIG=$(pwd)/local.config.yml
    # generate OpenAPI Document
    pygeoapi generate-openapi-document -c local.config.yml > openapi.yml
    export PYGEOAPI_OPENAPI=$(pwd)/openapi.yml

fi

pygeoapi serve