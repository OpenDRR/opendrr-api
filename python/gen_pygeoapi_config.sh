#!/bin/bash

# make sure you: chmod +x python/gen_config.sh

# generate the baseline configuration from ES indices
echo 'Generating the pygeoapi configuration...'
python gen_pygeoapi_config.py &&

# start up the local container
cd ../pygeoapi || exit

# build the image
echo 'Building the container image...'
docker build -t temp_image --no-cache .

# instantiate the container
echo 'Starting the container...'
docker run -d --name temp_container -p 5001:80 temp_image

# make sure pygeoapi is ready prior to creating indexes
until curl -sSf -XGET --insecure 'http://localhost:5001' > /dev/null; do
    printf 'pygeoapi not ready yet, trying again in 10 seconds \n'
    sleep 10
done

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

