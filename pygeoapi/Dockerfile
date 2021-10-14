FROM ghcr.io/opendrr/pygeoapi

LABEL maintainer="Joost van Ulden <joost.vanulden@canada.ca>"

# Volume mapping cannot be used with webhook
# https://github.com/maccyber/micro-dockerhub-hook
# simply copy config into Image.
COPY ./opendrr.config.yml /pygeoapi/local.config.yml
