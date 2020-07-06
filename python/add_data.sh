set -e

# make sure pg is ready to accept connections
until pg_isready -h db-opendrr -p 5432 -U postgres
do
  echo "Waiting for postgres..."
  sleep 2;
done

echo "\nImporting scenario outputs to PostGIS..."
python DSRA_outputs2postgres_lfs.py --dsraModelDir https://github.com/OpenDRR/openquake-models/tree/master/deterministic/outputs --columnsINI DSRA_outputs2postgres.ini &&
echo "\nGenerating indicators..."
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f /Create_scenario_risk_building_indicators_ALL.psql &&
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f /Create_scenario_risk_building_indicators_ALL_tables.psql &&
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f /Create_scenario_risk_sauid_indicators_ALL.psql &&
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f /Create_scenario_risk_sauid_indicators_ALL_tables.psql