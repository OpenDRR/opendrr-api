FROM ghcr.io/opendrr/python-env:1.2.3

LABEL maintainer="Joost van Ulden <joost.vanulden@canada.ca>" 

WORKDIR /usr/src/app

COPY . .

RUN chmod +x add_data.sh
CMD ["./add_data.sh"]
