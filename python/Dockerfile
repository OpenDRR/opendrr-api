FROM python:3-slim

LABEL maintainer="Joost van Ulden <joost.vanulden@canada.ca>"

WORKDIR /usr/src/app

# install psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc
RUN pip3 install psycopg2~=2.6
RUN apt-get autoremove -y gcc

# get the required scripts from GitHub repo
ADD https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/DSRA_outputs2postgres_lfs.py /usr/src/app
ADD https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/DSRA_outputs2postgres.ini /usr/src/app

# copy required files
COPY . .

RUN pip3 install --upgrade pip && pip install -r requirements.txt

ENV PYTHONUNBUFFERED 1

CMD ["python", "./DSRA_outputs2postgres_lfs.py", "--dsraModelDir", "https://github.com/OpenDRR/openquake-models/tree/master/deterministic/outputs", "--columnsINI", "DSRA_outputs2postgres.ini"]
