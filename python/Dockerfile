FROM opendrr/python-env

LABEL maintainer="Joost van Ulden <joost.vanulden@canada.ca>" 

WORKDIR /usr/src/app

COPY . .

#RUN chmod +x docker-entrypoint.sh && chmod +x add_data.sh
RUN chmod +x add_data.sh && \
    apt-get update && \
    apt-get install -y dos2unix eatmydata git git-lfs jq time && \
    dos2unix add_data.sh
#ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
CMD ./add_data.sh
