FROM debian:sid-slim

LABEL maintainer="Joost van Ulden <joost.vanulden@canada.ca>" 

WORKDIR /usr/src/app

# copy required files
COPY . .

# install psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc curl git-lfs gdal-bin python3-pip && \
    pip3 install psycopg2~=2.6 && \
    apt-get autoremove -y gcc && \
    apt-get install -y postgresql-client && \
    pip3 install --upgrade pip && pip install -r requirements.txt


########## get populate data scripts ##########
#DSRA Outputs
#ADD https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/DSRA_outputs2postgres_lfs.py /usr/src/app
#ADD https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/DSRA_outputs2postgres.ini /usr/src/app 

#Physical Exposure Model
#ADD https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/Create_table_canada_exposure.sql /usr/src/app
#ADD https://media.githubusercontent.com/media/OpenDRR/model-inputs/murray/exposure/general-building-stock/BldgExp_Canada.csv?token=ABXE4IGQFGCJHUJUAPDX3S27C65PQ /usr/src/app 

#SOVI
#ADD https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/Create_table_sovi_census_canada.sql /usr/src/app
#ADD https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/Create_table_sovi_index_canada_v2.sql /usr/src/app


# GMF 
#ADD https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/DSRA_gmf2postgres_lfs.py /usr/src/app
#ADD https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/DSRA_gmf2postgres.ini /usr/src/app

#VS30 
#ADD https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/Create_table_vs_30_BC_site_model.sql /usr/src/app
#ADD https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/Create_table_vs_30_BC_site_model_update.sql /usr/src/app
#ADD https://raw.githubusercontent.com/OpenDRR/model-inputs/develop/earthquake/sites/regions/vs30_BC_site_model.csv?token=ABXE4IESUGRQSZ2ZZUC5UI27EDBH2 /usr/src/app

#Rupture 
#ADD https://raw.githubusercontent.com/OpenDRR/model-factory/master/scripts/DSRA_ruptures2postgres.py /usr/src/app

# get model-factory scripts
RUN git clone https://github.com/OpenDRR/model-factory.git 

# get boundary files
RUN git clone https://github.com/OpenDRR/boundaries.git --depth 1 || (cd boundaries ; git pull)

# get scenario views scripts
#ADD https://raw.githubusercontent.com/OpenDRR/opendrr-data-store/master/scripts/Indicators/3.0%20Earthquake%20Scenario%20Risk%20(DSRA)/3.Final/Create_scenario_risk_building_indicators_ALL.psql /usr/src/app
#ADD https://raw.githubusercontent.com/OpenDRR/opendrr-data-store/master/scripts/Indicators/3.0%20Earthquake%20Scenario%20Risk%20(DSRA)/3.Final/Create_scenario_risk_sauid_indicators_ALL.psql /usr/src/app

# copy required files
COPY . .

RUN pip3 install --upgrade pip && pip install -r requirements.txt
    #chmod 777 /usr/src/app/vs30_BC_site_model.csv && \
    #chmod 777 /usr/src/app/BldgExp_Canada.csv

ENV PYTHONUNBUFFERED 1 

CMD chmod +x add_data.sh && ./add_data.sh