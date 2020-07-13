set -e

# make sure PostGIS is ready to accept connections
until pg_isready -h db-opendrr -p 5432 -U ${POSTGRES_USER}
do
  echo "Waiting for postgres..."
  sleep 2;
done

# create boundaries schema geometry tables from default geopackages.  Change ogr2ogr PATH / geopackage path if nessessary to run.
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_ADAUID.gpkg" -nln boundaries."Geometry ADAUID" -lco LAUNDER=NO 
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_CANADA.gpkg" -nln boundaries."Geometry CANADA" -lco LAUNDER=NO 
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_CDUID.gpkg" -nln boundaries."Geometry CDUID" -lco LAUNDER=NO 
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_CSDUID.gpkg" -nln boundaries."Geometry CSDUID" -lco LAUNDER=NO 
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_ERUID.gpkg" -nln boundaries."Geometry ERUID" -lco LAUNDER=NO 
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_FSAUID.gpkg" -nln boundaries."Geometry FSAUID" -lco LAUNDER=NO 
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_SAUID.gpkg" -nln boundaries."Geometry SAUID" -lco LAUNDER=NO 

echo "\nImporting scenario outputs into PostGIS..."
python DSRA_outputs2postgres_lfs.py --dsraModelDir https://github.com/OpenDRR/openquake-models/tree/master/deterministic/outputs --columnsINI DSRA_outputs2postgres.ini &&

echo "\nGenerating indicator views..."
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_scenario_risk_building_indicators_ALL.psql &&
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_scenario_risk_sauid_indicators_ALL.psql

# make sure Elasticsearch is ready prior to creating indexes
until $(curl -sSf -XGET --insecure 'http://elasticsearch-opendrr:9200/_cluster/health?wait_for_status=yellow' > /dev/null); do
    printf 'No status yellow from Elasticsearch, trying again in 10 seconds \n'
    sleep 10
done

echo "\nCreating elasticsearch indexes..."
# python dsra_postgres2es.py --eqScenario="sim6p8_cr2022_rlz_1" --retrofitPrefix="b0" --dbview="casualties_agg_view" --idField="Sauid" &&
# python exposure.py --type="buildings" --aggregation="building" --geometry=geom_point --idField="AssetID"
