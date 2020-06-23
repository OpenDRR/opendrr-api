# =================================================================
#
# Author: Joost van Ulden <joost.vanulden@canada.ca>
#
# Copyright (c) 2020 Government of Canada
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

FROM postgis/postgis

LABEL maintainer="Joost van Ulden <joost.vanulden@canada.ca>"

USER root

RUN apt-get update && apt-get -qq -y install curl \
    && apt-get -y install --reinstall libpq-dev \
    && apt update && apt install -y python3-pip \
    && pip3 install \
    certifi \
    chardet \
    elasticsearch \
    idna \
    numpy \
    pandas \
    psycopg2 \
    python-dateutil \
    pytz \
    requests \
    six \
    SQLAlchemy \
    urllib3 \
    && curl -O https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/DSRA_outputs2postgres_lfs.py \
    && curl -O https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/DSRA_outputs2postgres.ini

COPY create_db.sh /docker-entrypoint-initdb.d/create_db.sh
COPY schema.sql /schema.sql
COPY config.ini /config.ini
# COPY test_data.sql /test_data.sql
# COPY load_data.py /load_data.py
# RUN chmod +x /docker-entrypoint-initdb.d/create_db /load_data.py
RUN chmod +x /docker-entrypoint-initdb.d/create_db.sh

# Assign necessary permisssions to folders
# RUN useradd appuser && chown -R appuser /app

EXPOSE 5433
