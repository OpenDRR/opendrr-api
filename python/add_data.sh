set -e

# make sure pg is ready to accept connections
until pg_isready -h db-opendrr -p 5432 -U ${POSTGRES_USER}
do
  echo "Waiting for postgres..."
  sleep 2;
done

echo "\nImporting scenario outputs into PostGIS..."
python DSRA_outputs2postgres_lfs.py --dsraModelDir https://github.com/OpenDRR/openquake-models/tree/master/deterministic/outputs --columnsINI DSRA_outputs2postgres.ini &&
echo "\nGenerating indicators..."
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_scenario_risk_building_indicators_ALL.psql &&
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_scenario_risk_building_indicators_ALL_tables.psql &&
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_scenario_risk_sauid_indicators_ALL.psql &&
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_scenario_risk_sauid_indicators_ALL_tables.psql

# make sure elasticsearch is ready prior to creating indexes
until $(curl -sSf -XGET --insecure 'http://elasticsearch-opendrr:9200/_cluster/health?wait_for_status=yellow' > /dev/null); do
    printf 'No status yellow from Elasticsearch, trying again in 10 seconds \n'
    sleep 10
done

echo "\nCreating elasticsearch indexes..."
python dsra_postgres2es.py
