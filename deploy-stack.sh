#!/bin/bash -m
# -*- coding: utf-8 -*-

function check_dep {
    for i in "$@"; do
        command -v "$i" &> /dev/null || {
            echo -e "!! Please install $i first. Aborting." >&2
            exit 1
        }
    done
}

check_dep curl git python3 pip docker

ROOT=$( pwd )

# create the network
docker network create opendrr-net > /dev/null 2>&1

# start Elasticsearch
container_es=opendrr-api-elasticsearch

if [ $(docker inspect -f '{{.State.Running}}' $container_es) = "true" ]; then
    printf "\nElasticsearch container running. Stopping...\n"
    docker stop $container_es > /dev/null 2>&1
fi
docker rm $container_es > /dev/null 2>&1
printf "\nInitializing Elasticsearch container...\n\n"
docker run -d --network opendrr-net --name $container_es -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.6.2

spin='-\|/'
i=0
until $(curl --output /dev/null --silent --head --fail http://localhost:9200); do
    i=$(( (i+1) %4 ))
    printf "\r${spin:$i:1}"
    sleep .1
done
printf "\r "

# load sample data into Elasticsearch
if [ ! -d "$ROOT/venv" ]; then
    python3 -m venv "venv"
fi
source "$ROOT/venv/bin/activate"

printf "\nInstalling dependencies...\n"
pip install elasticsearch &&

printf "\nLoading data into Elasticsearch...\n"
python3 $ROOT/scripts/load_es_data.py $ROOT/sample-data/dsra_sim6p8_cr2022_rlz_1_b0_economic_loss_agg_view.geojson Sauid  &&
printf "\nData load complete!\n"

printf "\nDeactivating virtualenv...\n"
deactivate

# start pygeoapi
container_pygeoapi=opendrr-api-pygeoapi

if [ $(docker inspect -f '{{.State.Running}}' $container_pygeoapi) = "true" ]; then
    printf "\npygeoapi container running. Stopping...\n"
    docker stop $container_pygeoapi > /dev/null 2>&1
fi
docker rm $container_pygeoapi > /dev/null 2>&1
printf "\nInitializing pygeoapi container...\n\n"
docker pull geopython/pygeoapi
docker run -d --network opendrr-net --name $container_pygeoapi -p 5000:80 -v $ROOT/configuration/local.config.yml:/pygeoapi/local.config.yml -it geopython/pygeoapi &&

printf "\nDone!\n"
printf "\nElasticsearch: http://localhost:9200"
printf "\npygeoapi: http://localhost:5000\n\n"