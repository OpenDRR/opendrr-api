# =================================================================
#
# Authors: Just van den Broecke <justb4@gmail.com>>
#          Jorge Samuel Mendes de Jesus <jorge.dejesus@geocat.net>
#          Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2019 Just van den Broecke
# Copyright (c) 2019 Jorge Samuel Mendes de Jesus
# Copyright (c) 2020 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

FROM docker.elastic.co/elasticsearch/elasticsearch:7.7.1

LABEL maintainer="Joost van Ulden <joost.vanulden@canada.ca>"

ARG DATA_FOLDER=/usr/share/elasticsearch/data
ARG DATASET=dsra_sim6p8_cr2022_rlz_1_b0_economic_loss_agg_view.geojson

ENV PIP_PACKAGES="elasticsearch numpy urllib3" \
	YUM_PACKAGES="python-pip python-setuptools wget"

USER root

RUN yum --enablerepo=extras -y install epel-release \
	&& yum install -y ${YUM_PACKAGES} \
	&& yum install -y python-setuptools \
	&& pip install --upgrade pip && pip install ${PIP_PACKAGES} \
	&& yum clean packages

RUN wget https://raw.githubusercontent.com/OpenDRR/opendrr-api/master/sample-data/${DATASET} -O ${DATA_FOLDER}/${DATASET} \
    && wget https://raw.githubusercontent.com/OpenDRR/opendrr-api/master/scripts/load_es_data.py -O /load_es_data.py \
    && wget https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh -O bin/wait-for-it.sh \
    && chmod +x bin/wait-for-it.sh \
#	&& echo "transport.host: 0.0.0.0" >> config/elasticsearch.yml \
#	&& echo "network.bind_host: 0.0.0.0" >> config/elasticsearch.yml \
	&& echo "network.publish_host: 0.0.0.0" >> config/elasticsearch.yml

COPY docker-entrypoint.sh /docker-entrypoint.sh
COPY add_data.sh /add_data.sh
RUN chmod +x /*.sh

USER elasticsearch
# CMD ["elasticsearch"]

# EXPOSE 9200 9300

# CMD ["/usr/share/elasticsearch/bin/elasticsearch"]

ENTRYPOINT ["/docker-entrypoint.sh"]