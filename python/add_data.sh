#!/bin/bash
trap : TERM INT
set -e

POSTGRES_USER=$1
POSTGRES_PASS=$2
POSTGRES_PORT=$3
DB_NAME=$4

#get github token
GITHUB_TOKEN=`grep -o 'github_token = *.*' config.ini | cut -f2- -d=`

status_code=$(curl --write-out %{http_code} --silent --output /dev/null -H "Authorization: token ${GITHUB_TOKEN}" \
  -O \
  -L https://api.github.com/repos/OpenDRR/openquake-models/contents/deterministic/outputs)

if [[ "$status_code" -ne 200 ]] ; then
  echo "GitHub token is not valid! Exiting!"
  exit 0
fi

# make sure PostGIS is ready to accept connections
until pg_isready -h db-opendrr -p 5432 -U ${POSTGRES_USER}
do
  echo "Waiting for postgres..."
  sleep 2;
done

#get list of earthquake scenarios
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -O \
  -L https://api.github.com/repos/OpenDRR/openquake-models/contents/deterministic/outputs
EQSCENARIO_LIST=`grep -P -o '"name": "s_lossesbyasset_*.*r2' outputs | cut -f3- -d_`
EQSCENARIO_LIST=($(echo $EQSCENARIO_LIST | tr ' ' '\n'))
for item in ${!EQSCENARIO_LIST[@]}
do
EQSCENARIO_LIST[item]=${EQSCENARIO_LIST[item]:0:${#EQSCENARIO_LIST[item]}-3}
#EQSCENARIO_LIST[item]=${EQSCENARIO_LIST[item],,}
done
# EQSCENARIO_LIST=afm7p2_lrdmf

# get model-factory scripts
git clone https://github.com/OpenDRR/model-factory.git --depth 1 || (cd model-factory ; git pull)

# get boundary files
git clone https://github.com/OpenDRR/boundaries.git --depth 1 || (cd boundaries ; git pull)

# copy model-factory scripts to working directory
cp model-factory/scripts/*.* .
rm -rf model-factory

echo "\n Importing Census Boundaries"
# create boundaries schema geometry tables from default geopackages.  Change ogr2ogr PATH / geopackage path if nessessary to run.
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_ADAUID.gpkg" -t_srs "EPSG:4326" -nln boundaries."Geometry_ADAUID" -lco LAUNDER=NO
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_CANADA.gpkg" -t_srs "EPSG:4326" -nln boundaries."Geometry_CANADA" -lco LAUNDER=NO
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_CDUID.gpkg" -t_srs "EPSG:4326" -nln boundaries."Geometry_CDUID" -lco LAUNDER=NO
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_CSDUID.gpkg" -t_srs "EPSG:4326" -nln boundaries."Geometry_CSDUID" -lco LAUNDER=NO
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_ERUID.gpkg" -t_srs "EPSG:4326" -nln boundaries."Geometry_ERUID" -lco LAUNDER=NO
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_FSAUID.gpkg" -t_srs "EPSG:4326" -nln boundaries."Geometry_FSAUID" -lco LAUNDER=NO
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_SAUID.gpkg" -t_srs "EPSG:4326" -nln boundaries."Geometry_SAUID" -lco LAUNDER=NO
rm -rf boundaries
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Update_boundaries_SAUID_table.sql

echo "\n Importing scenario outputs into PostGIS..."
for eqscenario in ${EQSCENARIO_LIST[*]}
do
python3 DSRA_outputs2postgres_lfs.py --dsraModelDir=https://github.com/OpenDRR/openquake-models/tree/master/deterministic/outputs --columnsINI=DSRA_outputs2postgres.ini --eqScenario=$eqscenario
done

echo "\n Importing Physical Exposure Model into PostGIS"
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -o BldgExp_Canada.csv \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/exposure/general-building-stock/BldgExp_Canada.csv?ref=ab1b2d58dcea80a960c079ad2aff337bc22487c5

DOWNLOAD_URL=`grep -o '"download_url": *.*' BldgExp_Canada.csv | cut -f2- -d: | tr -d '"'| tr -d ',' `
curl -o BldgExp_Canada.csv \
  -L $DOWNLOAD_URL
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_table_canada_exposure.sql

echo "\n Importing VS30 Model into PostGIS..."
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -H "Accept: application/vnd.github.v3.raw+json" \
  -o vs30_BC_site_model.csv \
  -L https://api.github.com/repos/OpenDRR/model-inputs/git/blobs/2adfaa9cc7fea73562dfe6d4ef80675ca9172e31
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_table_vs_30_BC_site_model.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_table_vs_30_BC_site_model_update.sql

echo "\n Importing GMF Model"
for eqscenario in ${EQSCENARIO_LIST[*]}
do
python3 DSRA_gmf2postgres_lfs.py --gmfDir="https://github.com/OpenDRR/openquake-models/tree/master/deterministic/outputs"  --eqScenario=$eqscenario

echo "\n Importing Sitemesh"
python3 DSRA_sitemesh2postgres_lfs.py --sitemeshDir="https://github.com/OpenDRR/openquake-models/tree/master/deterministic/outputs" --eqScenario=$eqscenario

echo "\n Creating GMF Sitemesh xref"
python3 DSRA_sitemesh_gmf_xref.py  --eqScenario=$eqscenario
done

echo "\n Importing Rupture Model"
python3 DSRA_ruptures2postgres.py --dsraRuptureDir="https://github.com/OpenDRR/openquake-models/tree/master/deterministic/ruptures" 

echo "\n Importing Census Data"
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -o census-attributes-2016.csv \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/exposure/census-ref-sauid/census-attributes-2016.csv?ref=ab1b2d58dcea80a960c079ad2aff337bc22487c5
DOWNLOAD_URL=`grep -o '"download_url": *.*' census-attributes-2016.csv | cut -f2- -d: | tr -d '"'| tr -d ',' `
curl -o census-attributes-2016.csv \
  -L $DOWNLOAD_URL
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_table_2016_census_v3.sql


echo "\n Importing Sovi"
#need to source tables
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -O \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/social-vulnerability/social-vulnerability-census.csv
DOWNLOAD_URL=`grep -o '"download_url": *.*' social-vulnerability-census.csv | cut -f2- -d: | tr -d '"'| tr -d ',' `
curl -o social-vulnerability-census.csv \
  -L $DOWNLOAD_URL

curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -O \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/social-vulnerability/social-vulnerability-index.csv
DOWNLOAD_URL=`grep -o '"download_url": *.*' social-vulnerability-index.csv | cut -f2- -d: | tr -d '"'| tr -d ',' `
curl -o social-vulnerability-index.csv \
  -L $DOWNLOAD_URL

psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_table_sovi_index_canada_v2.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_table_sovi_census_canada.sql

echo "\n Importing LUTs"
#Collapse Probability
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -o collapse_probability.csv \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/exposure/general-building-stock/documentation/collapse_probability.csv?ref=73d15ca7e48291ee98d8a8dd7fb49ae30548f34e
DOWNLOAD_URL=`grep -o '"download_url": *.*' collapse_probability.csv | cut -f2- -d: | tr -d '"'| tr -d ',' `
curl -o collapse_probability.csv \
  -L $DOWNLOAD_URL
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_collapse_probability_table.sql
#Retrofit Costs
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -o retrofit_costs.csv \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/exposure/general-building-stock/documentation/retrofit_costs.csv?ref=73d15ca7e48291ee98d8a8dd7fb49ae30548f34e

DOWNLOAD_URL=`grep -o '"download_url": *.*' retrofit_costs.csv | cut -f2- -d: | tr -d '"'| tr -d ',' `
curl -o retrofit_costs.csv \
  -L $DOWNLOAD_URL
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_retrofit_costs_table.sql

echo "\n Importing GHSL"
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -o mh-intensity-ghsl.csv \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/natural-hazards/mh-intensity-ghsl.csv?ref=ab1b2d58dcea80a960c079ad2aff337bc22487c5
DOWNLOAD_URL=`grep -o '"download_url": *.*' mh-intensity-ghsl.csv | cut -f2- -d: | tr -d '"'| tr -d ',' `
curl -o mh-intensity-ghsl.csv \
  -L $DOWNLOAD_URL
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_table_GHSL.sql

echo "\n Importing MH Intensity"
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -o mh-intensity-sauid.csv \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/natural-hazards/mh-intensity-sauid.csv?ref=ab1b2d58dcea80a960c079ad2aff337bc22487c5
DOWNLOAD_URL=`grep -o '"download_url": *.*' mh-intensity-sauid.csv | cut -f2- -d: | tr -d '"'| tr -d ',' `
curl -o mh-intensity-sauid.csv \
  -L $DOWNLOAD_URL
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_table_mh_intensity_canada_v2.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_table_mh_thresholds.sql

echo "\n Generate Indicators"
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_physical_exposure_building_indicators_PhysicalExposure.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_physical_exposure_sauid_indicators_view_PhysicalExposure.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_risk_dynamics_indicators.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_social_vulnerability_sauid_indicators_SocialFabric.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_MH_risk_sauid_ALL.sql




echo "\n Generating indicator views..."
for eqscenario in ${EQSCENARIO_LIST[*]}
do
python3 DSRA_createRiskProfileIndicators.py --eqScenario=$eqscenario --aggregation=building
python3 DSRA_createRiskProfileIndicators.py --eqScenario=$eqscenario --aggregation=sauid
done 

# make sure Elasticsearch is ready prior to creating indexes
until $(curl -sSf -XGET --insecure 'http://elasticsearch-opendrr:9200/_cluster/health?wait_for_status=yellow' > /dev/null); do
    printf 'No status yellow from Elasticsearch, trying again in 10 seconds \n'
    sleep 10
done

for eqscenario in ${EQSCENARIO_LIST[*]}
do
echo "\nCreating elasticsearch indexes..."
python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="casualties" --idField="building"
python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="damage_state" --idField="building"
python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="economic_loss" --idField="building"
python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="recovery_time" --idField="building"
python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="scenario_shakemap_intensity" --idField="building"
python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="social_disruption" --idField="building"

python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="casualties" --idField="sauid"
python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="damage_state" --idField="sauid"
python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="economic_loss" --idField="sauid"
python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="recovery_time" --idField="sauid"
python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="scenario_shakemap_intensity" --idField="sauid"
python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="social_disruption" --idField="sauid"

done

echo "\n Loading Kibana Saved Objects"
curl -X POST http://kibana-opendrr:5601/api/saved_objects/_import -H "kbn-xsrf: true" --form file=@kibanaSavedObjects.ndjson

tail -f /dev/null & wait