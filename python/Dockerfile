FROM ghcr.io/anthonyfok/python-env:debian-sid-20201012

LABEL maintainer="Joost van Ulden <joost.vanulden@canada.ca>" 

WORKDIR /usr/src/app

COPY . .

#RUN chmod +x docker-entrypoint.sh && chmod +x add_data.sh
RUN chmod +x add_data.sh && \
    dos2unix add_data.sh
#ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
CMD ./add_data.sh
