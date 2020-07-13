FROM debian:sid-slim

LABEL maintainer="Joost van Ulden <joost.vanulden@canada.ca>"

WORKDIR /usr/src/app

# install psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc curl git-lfs gdal-bin python3-pip
RUN pip3 install psycopg2~=2.6
RUN apt-get autoremove -y gcc
RUN apt-get install -y postgresql-client

# get populate data scripts
ADD https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/DSRA_outputs2postgres_lfs.py /usr/src/app
ADD https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/DSRA_outputs2postgres.ini /usr/src/app

# get boundary files
RUN git clone https://github.com/OpenDRR/boundaries.git --depth 1 || (cd boundaries ; git pull)

# get scenario views scripts
# ADD https://raw.githubusercontent.com/OpenDRR/opendrr-data-store/master/scripts/Sandbox/Indicators/3.0%20Earthquake%20Scenario%20Risk%20(DSRA)/1.scenario_risk/v5%20updated%20metrics/6dp%20numeric/Create_scenario_risk_building_indicators_ALL.psql /usr/src/app
# ADD https://raw.githubusercontent.com/OpenDRR/opendrr-data-store/master/scripts/Sandbox/Indicators/3.0%20Earthquake%20Scenario%20Risk%20(DSRA)/1.scenario_risk/v5%20updated%20metrics/6dp%20numeric/Create_scenario_risk_building_indicators_ALL_tables.psql /usr/src/app
# ADD https://raw.githubusercontent.com/OpenDRR/opendrr-data-store/master/scripts/Sandbox/Indicators/3.0%20Earthquake%20Scenario%20Risk%20(DSRA)/1.scenario_risk/v5%20updated%20metrics/6dp%20numeric/Create_scenario_risk_sauid_indicators_ALL.psql /usr/src/app
# ADD https://raw.githubusercontent.com/OpenDRR/opendrr-data-store/master/scripts/Sandbox/Indicators/3.0%20Earthquake%20Scenario%20Risk%20(DSRA)/1.scenario_risk/v5%20updated%20metrics/6dp%20numeric/Create_scenario_risk_sauid_indicators_ALL_tables.psql /usr/src/app

# copy required files
COPY . .

# DEBUG - list files
RUN ls -l *

RUN ogr2ogr --version

RUN pip3 install --upgrade pip && pip install -r requirements.txt

ENV PYTHONUNBUFFERED 1 

CMD chmod +x add_data.sh && ./add_data.sh
