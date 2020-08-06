FROM opendrr/python-env

LABEL maintainer="Joost van Ulden <joost.vanulden@canada.ca>" 

WORKDIR /usr/src/app

COPY . .

RUN chmod +x docker-entrypoint.sh && chmod +x add_data.sh

ENTRYPOINT ["docker-entrypoint.sh"]