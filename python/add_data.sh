set -e

# make sure PostGIS is ready to accept connections
until pg_isready -h db-opendrr -p 5432 -U ${POSTGRES_USER}
do
  echo "Waiting for postgres..."
  sleep 2;
done

GITHUB_TOKEN=`grep -o 'github_token = *.*' config.ini | cut -f2- -d=`

#ls model-factory/scripts/ -ltr

cp model-factory/scripts/*.* .

echo "\n Importing Census Boundaries"
# create boundaries schema geometry tables from default geopackages.  Change ogr2ogr PATH / geopackage path if nessessary to run.
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_ADAUID.gpkg" -nln boundaries."Geometry ADAUID" -lco LAUNDER=NO 
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_CANADA.gpkg" -nln boundaries."Geometry CANADA" -lco LAUNDER=NO 
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_CDUID.gpkg" -nln boundaries."Geometry CDUID" -lco LAUNDER=NO 
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_CSDUID.gpkg" -nln boundaries."Geometry CSDUID" -lco LAUNDER=NO 
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_ERUID.gpkg" -nln boundaries."Geometry ERUID" -lco LAUNDER=NO 
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_FSAUID.gpkg" -nln boundaries."Geometry FSAUID" -lco LAUNDER=NO 
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_SAUID.gpkg" -nln boundaries."Geometry SAUID" -lco LAUNDER=NO 

echo "\n Importing scenario outputs into PostGIS..."
python3 DSRA_outputs2postgres_lfs.py --dsraModelDir=https://github.com/OpenDRR/openquake-models/tree/master/deterministic/outputs --columnsINI=DSRA_outputs2postgres.ini 




echo "\n Importing Physical Exposure Model into PostGIS"
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -O \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/exposure/general-building-stock/BldgExp_Canada.csv 
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
python3 DSRA_gmf2postgres_lfs.py --gmfDir="https://github.com/OpenDRR/openquake-models/tree/master/deterministic/outputs"  --eqScenario=AFM7p2_LRDMF

echo "\n Importing Sitemesh"
python3 DSRA_sitemesh2postgres_lfs.py --sitemeshDir="https://github.com/OpenDRR/openquake-models/tree/master/deterministic/outputs" --eqScenario=AFM7p2_LRDMF

echo "\n Creating GMF Sitemesh xref"
python3 DSRA_sitemesh_gmf_xref.py  --eqScenario=afm7p2_lrdmf

echo "\n Importing Rupture Model"
python3 DSRA_ruptures2postgres.py --dsraRuptureDir="https://github.com/OpenDRR/openquake-models/tree/master/deterministic/ruptures" 

echo "\n Importing Census Data"
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -O \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/exposure/census-ref-sauid/census-attributes-2016.csv
  
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
  -O \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/exposure/general-building-stock/documentation/collapse_probability.csv
DOWNLOAD_URL=`grep -o '"download_url": *.*' collapse_probability.csv | cut -f2- -d: | tr -d '"'| tr -d ',' `
curl -o collapse_probability.csv \
  -L $DOWNLOAD_URL
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_collapse_probability_table.sql
#Retrofit Costs
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -O \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/exposure/general-building-stock/documentation/retrofit_costs.csv
DOWNLOAD_URL=`grep -o '"download_url": *.*' retrofit_costs.csv | cut -f2- -d: | tr -d '"'| tr -d ',' `
curl -o retrofit_costs.csv \
  -L $DOWNLOAD_URL
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_retrofit_costs_table.sql


echo "\n Generating indicator views..."
#psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_scenario_risk_building_indicators_ALL.psql &&
#psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_scenario_risk_sauid_indicators_ALL.psql
python3 DSRA_createRiskProfileIndicators.py --eqScenario=afm7p2_lrdmf --aggregation=building
python3 DSRA_createRiskProfileIndicators.py --eqScenario=afm7p2_lrdmf --aggregation=sauid

# make sure Elasticsearch is ready prior to creating indexes
# until $(curl -sSf -XGET --insecure 'http://elasticsearch-opendrr:9200/_cluster/health?wait_for_status=yellow' > /dev/null); do
#     printf 'No status yellow from Elasticsearch, trying again in 10 seconds \n'
#     sleep 10
# done

echo "\nCreating elasticsearch indexes..."
# python dsra_postgres2es.py --eqScenario="sim6p8_cr2022_rlz_1" --retrofitPrefix="b0" --dbview="casualties_agg_view" --idField="Sauid" &&
# python exposure.py --type="buildings" --aggregation="building" --geometry=geom_point --idField="AssetID"
