FROM opendrr/python-env

LABEL maintainer="Joost van Ulden <joost.vanulden@canada.ca>" 

WORKDIR /usr/src/app

COPY . .

#RUN chmod +x docker-entrypoint.sh && chmod +x add_data.sh
RUN chmod +x add_data.sh && \
    apt-get update && \
    apt-get install -y eatmydata git git-lfs jq time zip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install
#ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
CMD ./add_data.sh
