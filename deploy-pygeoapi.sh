#!/bin/bash -m
# -*- coding: utf-8 -*-

ROOT=$( pwd )
OPTIND=1

function print_help
{
  echo '
    Deploy the application.

    Usage:
    -c          clean install
    -h          show help
    '
}

while getopts "ch" opt; do
  case ${opt} in
    c)
      clean=1
      ;;
    \?|h)
      print_help
      exit 0
      ;;
  esac
done

# clean up previous cloned repo
if [  "${clean}" ]; then
    echo "Starting a clean install..."
    rm -r -f pygeoapi
fi

# Stop any running process
lsof -ti:5000 | xargs kill

if [ ! -d "$ROOT/venv" ]; then
    python3 -m venv "venv"
fi
source "$ROOT/venv/bin/activate"

cd venv
. bin/activate

# clean up previous cloned repo
if [  "${clean}" ]; then
    echo "Starting a clean install..."
    rm -r -f pygeoapi
fi

if [ ! -d "pygeoapi" ]; then

    git clone https://github.com/geopython/pygeoapi.git
    cd pygeoapi
    pip install -r requirements.txt
    pip install -r requirements-dev.txt
    pip install -r requirements-provider.txt
    pip install -e .
    # generate OpenAPI Document
    pygeoapi generate-openapi-document -c local.config.yml > openapi.yml

fi

# copy local configuration
cp $ROOT/configuration/local.config.yml local.config.yml

export PYGEOAPI_CONFIG=$(pwd)/local.config.yml
export PYGEOAPI_OPENAPI=$(pwd)/openapi.yml

pygeoapi serve