FROM ghcr.io/opendrr/python-env:1.2.6

LABEL maintainer="Joost van Ulden <joost.vanulden@canada.ca>" 

WORKDIR /usr/src/app

COPY . .

RUN chmod +x add_data.sh
CMD ["./add_data.sh"]
# RUN chmod +x build_exposure_ancillary.sh
# CMD ["./build_exposure_ancillary.sh"]
