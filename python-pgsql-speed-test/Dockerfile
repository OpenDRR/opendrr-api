FROM opendrr/python-env

LABEL maintainer="Joost van Ulden <joost.vanulden@canada.ca>" 

WORKDIR /usr/src/app

COPY . .

#RUN chmod +x docker-entrypoint.sh && chmod +x add_data.sh
RUN chmod +x test_pgsql.sh && \
    apt-get update && \
    apt-get install -y eatmydata jq time
#ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
CMD ./test_pgsql.sh ${POSTGRES_USER} ${POSTGRES_PASS} ${POSTGRES_PORT} ${DB_NAME} ${POSTGRES_HOST} ${ES_ENDPOINT} ${ES_USER} ${ES_PASS} ${KIBANA_ENDPOINT}
