#!/bin/bash

# make sure you: chmod +x python/gen_config.sh

# generate the baseline configuration from ES indices
echo 'Generating the pygeoapi configuration...'
python gen_pygeoapi_config.py &&

# start up the local container
cd ../pygeoapi

# build the image
echo 'Building the container image...'
docker build -t temp_image --no-cache .

# instantiate the container
echo 'Starting the container...'
docker run -d --name temp_container -p 5000:80 temp_image

# copy the openapi specfication document
echo 'Copying the openapi configuration file...'
docker cp temp_container:/pygeoapi/local.openapi.yml opendrr.openapi.yml

# shut down the container and delete it
echo 'Shutting down the container and removing it...'
docker stop temp_container
docker rm temp_container

# remove the image
docker rmi temp_image

echo 'Done!'

