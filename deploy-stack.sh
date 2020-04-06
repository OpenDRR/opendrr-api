#!/bin/bash -m
# -*- coding: utf-8 -*-

ROOT=$( pwd )

# create the network
docker network create opendrr-net

# start Elasticsearch
container_es=opendrr-api-elasticsearch

if [ $(docker inspect -f '{{.State.Running}}' $container_es) = "true" ]; then
    printf "\nElasticsearch container running...\n"
    docker stop $container_es
else 
    docker rm $container_es
    printf "\nInitializing Elasticsearch container...\n"
    docker run --network opendrr-net --name $container_es -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.6.2
fi

# load sample data into Elasticsearch
printf "\nLoading data into Elasticsearch...\n"
python3 $ROOT/scripts/load_es_data.py $ROOT/sample-data/dsra_sim6p8_cr2022_rlz_1_b0_economic_loss_agg_view.geojson Sauid &&

# start pygeoapi
container_pygeoapi=opendrr-api-pygeoapi

if [ $(docker inspect -f '{{.State.Running}}' $container_pygeoapi) = "true" ]; then
    printf "\npygeoapi container running...\n"
    docker stop $container_pygeoapi
else 
    docker rm $container_pygeoapi
    printf "\nInitializing pygeoapi container...\n"
    docker pull geopython/pygeoapi
    docker run --network opendrr-net --name $container_pygeoapi -p 5000:80 -v $ROOT/configuration/local.config.yml:/pygeoapi/local.config.yml -it geopython/pygeoapi
fi
