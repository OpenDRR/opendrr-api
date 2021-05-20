#!/bin/bash
# SPDX-License-Identifier: MIT
#
# add_data.sh - Populate PostGIS database for Elasticsearch
#
# Copyright (C) 2020-2021 Government of Canada
#
# Main Authors: Drew Rotheram-Clarke <drew.rotheram-clarke@canada.ca>
#               Joost van Ulden <joost.vanulden@canada.ca>

trap : TERM INT
set -e

POSTGRES_USER=$1
POSTGRES_PASS=$2
POSTGRES_PORT=$3
DB_NAME=$4
POSTGRES_HOST=$5

ES_ENDPOINT=$6
ES_USER=$7
ES_PASS=$8
KIBANA_ENDPOINT=$9

DSRA_REPOSITORY=https://github.com/OpenDRR/scenario-catalogue/tree/master/FINISHED

############################################################################################
#######################     Define helper functions                  #######################
############################################################################################

# LOG prints log message while preserving quoting
LOG() {
  echo -n "[add_data]"
  for i in "$@"; do
    i="${i/$GITHUB_TOKEN/***}"
    if echo "$i" | grep -q ' '; then
      echo "$i" | grep -q "'" && i="\"$i\"" || i="'$i'"
    fi
    echo -n " $i"
  done
  echo
}

# RUN runs a command, logs, and prints timing and memory information
RUN() {
  LOG "$@"
  if [ -n "$(type -p $1)" ]; then
    time /usr/bin/time "$@"	# file
  else
    time "$@"	# alias, keyword, function, or builtin
  fi
}

# set_synchronous_commit sets database's synchronous_commit
# to "off" for speed, or to "on" for reliability.
set_synchronous_commit() {
  if [ "$#" -ne 1 ]; then
    echo "ERROR: ${FUNCNAME[0]} requires exactly one argument, but $# was given."
    exit 1
  fi
  local on_off="$1"

  echo "  - psql: Setting synchronous_commit TO $on_off..."
  psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" -a \
    -c "SHOW synchronous_commit;"
  psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" -a \
    -c "ALTER DATABASE $DB_NAME SET synchronous_commit TO $on_off;"
  psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" -a \
    -c "SHOW synchronous_commit;"
}

# run_ogr2ogr creates boundaries schema geometry tables from default geopackages.
# (Change ogr2ogr PATH / geopackage path if necessary to run.)
run_ogr2ogr() {
  if [ "$#" -ne 1 ]; then
    echo "Error: run_ogr2ogr() requires exactly one argument, but $# was given."
    exit 1
  fi
  local id="$1"

  local srs_def="EPSG:4326"
  local dst_datasource_name=PG:"host='$POSTGRES_HOST' user='$POSTGRES_USER' dbname='$DB_NAME' password='$POSTGRES_PASS'"
  local src_datasource_name="boundaries/$id.gpkg"
  local nln="boundaries.$(basename $id)"

  echo "  - ogr2ogr: Importing $src_datasource_name into $DB_NAME..."

  ogr2ogr -t_srs "$srs_def" \
	  -f PostgreSQL \
	  "$dst_datasource_name" \
	  "$src_datasource_name" \
	  -lco LAUNDER=NO \
	  -nln "$nln"
}

# run_psql runs PostgreSQL queries from a given input SQL file.
run_psql() {
  if [ "$#" -ne 1 ]; then
    echo "Error: run_psql() requires exactly one argument, but $# was given."
    exit 1
  fi
  local input_file="$1"

  echo "  - psql: Running $input_file..."
  psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" -a -f "$input_file"
}


# fetch_csv_lfs downloads CSV data files from OpenDRR repos
# with help from GitHub API with support for LFS files.
# See https://docs.github.com/en/rest/reference/repos#get-repository-content
fetch_csv_lfs() {
  if [ "$#" -ne 2 ]; then
    echo "Error: ${FUNCNAME[0]} requires exactly two arguments, but $# was given."
    exit 1
  fi
  local owner="OpenDRR"
  local repo="$1"
  local path="$2"
  local output_file=$(basename $path | sed -e 's/?.*//')
  local response="github-api/$2.json"

  mkdir -p github-api/$(dirname $path)

  echo $repo/$path
  curl -s -o "$response" \
    --retry 999 --retry-max-time 0 \
    -H "Authorization: token ${GITHUB_TOKEN}" \
    -L "https://api.github.com/repos/$owner/$repo/contents/$path"

  local download_url=$(jq -r '.download_url' "$response")
  local size=$(jq -r '.size' "$response")

  echo download_url=$download_url
  echo size=$size

  echo "${FUNCNAME[0]}: Download from $download_url"
  curl -o "$output_file" -L "$download_url" --retry 999 --retry-max-time 0
}

# fetch_csv_xz downloads CSV data files from OpenDRR xz-compressed repos
fetch_csv_xz() {
  if [ "$#" -ne 2 ]; then
    echo "ERROR: ${FUNCNAME[0]} requires exactly two arguments, but $# was given."
    exit 1
  fi
  local owner="OpenDRR"
  local repo="$1"
  local path="$2"
  local output_file=$(basename $path | sed -e 's/?.*//')
  local response
  local path_dir=$(dirname "$path")
  echo $path_dir

  # Fetch directory listing
  mkdir -p github-api/$path_dir
  response="github-api/$path_dir.dir.json"
  curl -s -o "$response" \
    --retry 999 --retry-max-time 0 \
    -H "Authorization: token ${GITHUB_TOKEN}" \
    -H "Accept: application/vnd.github.v3+json" \
    -L "https://api.github.com/repos/$owner/$repo-xz/contents/$path_dir"

  local download_url=$(jq -r '.[] | select(.name == "'"$output_file"'.xz") | .download_url' $response)
  echo "${FUNCNAME[0]}: Download from $download_url"
  curl -o "$output_file.xz" -L "$download_url" --retry 999 --retry-max-time 0

  # TODO: Keep the compressed file somewhere, uncompress when needed
  unxz $output_file.xz
}

# fetch_csv calls either fetch_csv_xz or fetch_csv_lfs to fetch CSV files
fetch_csv() {
  # TODO: Make it more intelligent.
  RUN fetch_csv_xz "$@" || RUN fetch_csv_lfs "$@"
}


# fetch_psra_csv_from_model fetches CSV files from the specified model
# for all provinces and territories.
fetch_psra_csv_from_model() {
  if [ "$#" != "1" ]; then
    echo "ERROR: ${FUNCNAME[0]} requires exactly one argument."
    exit 1
  fi

  model=$1

  for PT in ${PT_LIST[@]}; do
    curl -H "Authorization: token ${GITHUB_TOKEN}" \
      --retry 999 --retry-max-time 0 \
      -o ${PT}.json \
      -L https://api.github.com/repos/OpenDRR/canada-srm2/contents/$model/output/${PT}

    DOWNLOAD_LIST=($(jq -r '.[].url | select(. | contains(".csv"))' ${PT}.json))

    mkdir -p $model/${PT}
    ( cd $model/${PT}
      for file in ${DOWNLOAD_LIST[@]}; do
        FILENAME=$(echo $file | cut -f-1 -d? | cut -f11- -d/)
        curl -H "Authorization: token ${GITHUB_TOKEN}" \
          --retry 999 --retry-max-time 0 \
          -o $FILENAME \
          -L $file
        DOWNLOAD_URL=$(jq -r '.download_url' $FILENAME)
        curl -o $FILENAME \
          --retry 999 --retry-max-time 0 \
          -L $DOWNLOAD_URL

        # Strip OpenQuake comment header if exists
        # (safe for cH_${PT}_hmaps_xref.csv)
        sed -i -r $'1{/^(\xEF\xBB\xBF)?#,/d}' $FILENAME
      done
      # TODO: Use a different for ${PT}.json, and keep for debugging
      rm -f ${PT}.json
    )
  done
}

# merge_csv merges CSV files without repeating column headers.
# The '#,,,,,"generated_by='OpenQuake engine 3.x..."' header is removed too.
# Syntax: merge_cvs [INPUT_CSV_FILES]... [OUTPUT_FILE]
merge_csv() {
  if [ "$#" -lt "2" ]; then
    echo "ERROR: ${FUNCNAME[0]} needs at least two arguments"
    exit 1
  fi
  input_files="${@:1:$#-1}"
  output_file="${@:$#}"

  echo "merge_cvs input: $input_files"
  echo "merge_cvs output: $output_file"

  if [ "$#" = "2" -a "$1" = "$2" ]; then
    echo "NOTICE: There is only one input file, and it has the same name as output file, skipping."
    return
  fi

  if echo "$input_files" | grep -q "$output_file"; then
    echo "ERROR: Output file \"$output_file\" is listed among input files: \"$input_files\""
    exit 1
  fi

  if [ -e "$output_file" ]; then
    echo "WARNING: Output file \"$output_file\" already exists!  Overwriting..."
  fi

  # The "awk" magic that merge CSV files while stripping duplicated headers.
  # See https://apple.stackexchange.com/questions/80611/merging-multiple-csv-files-without-merging-the-header
  #awk '(NR == 2) || (FNR > 2)' $input_files > "$output_file" # NOTE: Do not quote $input_files here!
  awk '(NR == 1) || (FNR > 1)' $input_files > "$output_file" # NOTE: Do not quote $input_files here!

  return
}

############################################################################################
#######################     Begin main processes                     #######################
############################################################################################

# Read GitHub token from config.ini
GITHUB_TOKEN=$(sed -n -e 's/github_token *= *\([0-9A-Fa-f]\+\)/\1/p' config.ini)

status_code=$(curl --write-out %{http_code} --silent --output /dev/null -H "Authorization: token ${GITHUB_TOKEN}" \
  -O \
  -L https://api.github.com/repos/OpenDRR/scenario-catalogue/contents/deterministic/outputs)

if [[ "$status_code" -ne 200 ]] ; then
  echo "GitHub token is not valid! Exiting!"
  exit 0
fi

# Make sure PostGIS is ready to accept connections
until pg_isready -h ${POSTGRES_HOST} -p 5432 -U ${POSTGRES_USER}
do
  echo "Waiting for postgres..."
  sleep 2;
done

# Get model-factory scripts
git clone https://github.com/OpenDRR/model-factory.git --depth 1 || (cd model-factory ; git pull)

# Copy model-factory scripts to working directory
cp model-factory/scripts/*.* .
#rm -rf model-factory


# Speed up PostgreSQL operations
set_synchronous_commit off

############################################################################################
#######################     Process Exposure and Ancillary Data      #######################
############################################################################################

echo -e "\n Importing Census Boundaries"

echo " - Trying to download pre-generated PostGIS database dump (for speed)..."
if RUN curl -O -v --retry 999 --retry-max-time 0 https://opendrr.eccp.ca/file/OpenDRR/opendrr-boundaries.dump || \
   RUN curl -O -v --retry 999 --retry-max-time 0 https://f000.backblazeb2.com/file/OpenDRR/opendrr-boundaries.dump
then
   RUN pg_restore -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" \
    --verbose --clean --if-exists --create opendrr-boundaries.dump \
    | while IFS= read -r line; do printf '%s %s\n' "$(date "+%Y-%m-%d %H:%M:%S")" "$line"; done
else
  echo " - WARNING: Unable to fetch pre-generated PostGIS database dump."
  echo "            Fallback to fetching boundaries CSV from GitHub (LFS)..."

  # Get boundary files
  RUN git clone https://github.com/OpenDRR/boundaries.git --depth 1 || (cd boundaries ; RUN git pull)

  # Create boundaries schema geometry tables from default geopackages.
  for i in ADAUID CANADA CDUID CSDUID DAUID ERUID FSAUID PRUID SAUID; do
    RUN run_ogr2ogr "Geometry_$i"
  done

  for i in HexGrid_5km HexGrid_10km HexGrid_25km HexGrid_50km SAUID_HexGrid; do
    RUN run_ogr2ogr "hexbin_4326/$i"
  done

  #rm -rf boundaries
fi

RUN run_psql Update_boundaries_SAUID_table.sql


# Physical Exposure
echo -e "\n Importing Physical Exposure Model into PostGIS"
RUN fetch_csv model-inputs \
  exposure/general-building-stock/BldgExpRef_CA_master_v3p1.csv
RUN run_psql Create_table_canada_exposure.sql

RUN fetch_csv model-inputs \
  exposure/building-inventory/metro-vancouver/PhysExpRef_MetroVan_v4.csv
RUN run_psql Create_table_canada_site_exposure_ste.sql

# VS30
echo -e "\n Importing VS30 Model into PostGIS..."
RUN fetch_csv model-inputs \
  earthquake/sites/regions/vs30_CAN_site_model_xref.csv

RUN fetch_csv model-inputs \
  earthquake/sites/regions/site-vgrid_CA.csv

RUN run_psql Create_table_vs_30_CAN_site_model.sql
RUN run_psql Create_table_vs_30_CAN_site_model_xref.sql

# Census Data
echo -e "\n Importing Census Data"
RUN fetch_csv model-inputs \
  exposure/census-ref-sauid/census-attributes-2016.csv?ref=ab1b2d58dcea80a960c079ad2aff337bc22487c5
RUN run_psql Create_table_2016_census_v3.sql


echo -e "\n Importing Sovi"
# Need to source tables
RUN fetch_csv model-inputs \
  social-vulnerability/social-vulnerability-census_2021.csv
RUN fetch_csv model-inputs \
  social-vulnerability/social-vulnerability-index_2021.csv
RUN fetch_csv model-inputs \
  social-vulnerability/sovi_thresholds_2021.csv

RUN run_psql Create_table_sovi_index_canada_v2.sql
RUN run_psql Create_table_sovi_census_canada.sql
RUN run_psql Create_table_sovi_thresholds.sql

echo -e "\n Importing LUTs"
RUN fetch_csv model-inputs \
  exposure/general-building-stock/documentation/collapse_probability.csv?ref=73d15ca7e48291ee98d8a8dd7fb49ae30548f34e
RUN run_psql Create_collapse_probability_table.sql

# Retrofit Costs
RUN fetch_csv model-inputs \
  exposure/general-building-stock/documentation/retrofit_costs.csv?ref=73d15ca7e48291ee98d8a8dd7fb49ae30548f34e
RUN run_psql Create_retrofit_costs_table.sql

echo -e "\n Importing GHSL"
RUN fetch_csv model-inputs \
  natural-hazards/mh-intensity-ghsl.csv?ref=ab1b2d58dcea80a960c079ad2aff337bc22487c5
RUN run_psql Create_table_GHSL.sql

echo -e "\n Importing MH Intensity"
RUN fetch_csv model-inputs \
  natural-hazards/HTi_sauid_2021.csv

echo -e "\n Importing Hazard Threat Thresholds"
RUN fetch_csv model-inputs \
  natural-hazards/HTi_thresholds_2021.csv

RUN run_psql Create_table_mh_intensity_canada_v2.sql
RUN run_psql Create_table_mh_thresholds.sql
RUN run_psql Create_MH_risk_building_ALL.sql
RUN run_psql Create_MH_risk_sauid_ALL.sql

# Use python to run \copy from a system call
RUN python3 copyAncillaryTables.py



# Perform update operations on all tables after data copied into tables
RUN run_psql Create_all_tables_update.sql
RUN run_psql Create_site_exposure_to_building_and_sauid.sql
RUN run_psql Create_table_vs_30_BC_CAN_model_update_site_exposure.sql

echo -e "\n Generate Indicators"
RUN run_psql Create_physical_exposure_building_indicators_PhysicalExposure.sql
RUN run_psql Create_physical_exposure_sauid_indicators_view_PhysicalExposure.sql
RUN run_psql Create_physical_exposure_building_indicators_PhysicalExposure_ste.sql
RUN run_psql Create_physical_exposure_sauid_indicators_view_PhysicalExposure_ste.sql
RUN run_psql Create_physical_exposure_site_level_indicators_PhysicalExposure_ste.sql
RUN run_psql Create_risk_dynamics_indicators.sql
RUN run_psql Create_social_vulnerability_sauid_indicators_SocialFabric.sql
RUN run_psql Create_MH_risk_sauid_ALL.sql


############################################################################################
#######################     Process PSRA                             #######################
############################################################################################

echo "Importing Raw PSRA Tables"

# Get list of provinces & territories
RUN curl -H "Authorization: token ${GITHUB_TOKEN}" \
  --retry 999 --retry-max-time 0 \
  -o output.json \
  -L https://api.github.com/repos/OpenDRR/canada-srm2/contents/cDamage/output

# EXPECTED_PT_LIST=(AB BC MB NB NL NS NT NU ON PE QC SK)

PT_LIST=($(jq -r '.[].name' output.json))

# cDamage
RUN fetch_psra_csv_from_model cDamage

for PT in ${PT_LIST[@]}; do
  ( cd cDamage/$PT
    RUN merge_csv cD_*dmg-mean_b0.csv cD_${PT}_dmg-mean_b0.csv
    RUN merge_csv cD_*dmg-mean_r2.csv cD_${PT}_dmg-mean_r2.csv
  )
done

# cHazard
RUN fetch_psra_csv_from_model cHazard

for PT in ${PT_LIST[@]}; do
  ( cd cHazard/$PT
    RUN python3 /usr/src/app/PSRA_hCurveTableCombine.py --hCurveDir=/usr/src/app/cHazard/${PT}/
  )
done

# eDamage
RUN fetch_psra_csv_from_model eDamage

for PT in ${PT_LIST[@]}; do
  ( cd eDamage/$PT
    RUN merge_csv eD_*damages-mean_b0.csv eD_${PT}_damages-mean_b0.csv
    RUN merge_csv eD_*damages-mean_r2.csv eD_${PT}_damages-mean_r2.csv
  )
done

# ebRisk
RUN fetch_psra_csv_from_model ebRisk

for PT in ${PT_LIST[@]}; do
  ( cd ebRisk/$PT
    RUN merge_csv ebR_*agg_curves-stats_b0.csv ebR_${PT}_agg_curves-stats_b0.csv
    RUN merge_csv ebR_*agg_curves-stats_r2.csv ebR_${PT}_agg_curves-stats_r2.csv
    RUN merge_csv ebR_*agg_losses-stats_b0.csv ebR_${PT}_agg_losses-stats_b0.csv
    RUN merge_csv ebR_*agg_losses-stats_r2.csv ebR_${PT}_agg_losses-stats_r2.csv
    RUN merge_csv ebR_*avg_losses-stats_b0.csv ebR_${PT}_avg_losses-stats_b0.csv
    RUN merge_csv ebR_*avg_losses-stats_r2.csv ebR_${PT}_avg_losses-stats_r2.csv

    # Combine source loss tables for runs that were split by economic region or sub-region
    python3 /usr/src/app/PSRA_combineSrcLossTable.py --srcLossDir=/usr/src/app/ebRisk/${PT}
  )
done

# PSRA_0
RUN run_psql psra_0.create_psra_schema.sql

# PSRA_1-8
for PT in ${PT_LIST[@]}
do
  RUN python3 PSRA_runCreate_tables.py --province=${PT} --sqlScript="psra_1.Create_tables.sql"
  RUN python3 PSRA_copyTables.py --province=${PT}
  RUN python3 PSRA_sqlWrapper.py --province=${PT} --sqlScript="psra_2.Create_table_updates.sql"
  RUN python3 PSRA_sqlWrapper.py --province=${PT} --sqlScript="psra_3.Create_psra_building_all_indicators.sql"
  RUN python3 PSRA_sqlWrapper.py --province=${PT} --sqlScript="psra_4.Create_psra_sauid_all_indicators.sql"
  RUN python3 PSRA_sqlWrapper.py --province=${PT} --sqlScript="psra_5.Create_psra_sauid_references_indicators.sql"
done

RUN run_psql psra_6.Create_psra_merge_into_national_indicators.sql

############################################################################################
#######################     Process DSRA                             #######################
############################################################################################

echo "Process DSRA"

# Get list of earthquake scenarios
RUN curl -H "Authorization: token ${GITHUB_TOKEN}" \
  --retry 999 --retry-max-time 0 \
  -o FINISHED.json \
  -L https://api.github.com/repos/OpenDRR/scenario-catalogue/contents/FINISHED

# s_lossesbyasset_ACM6p5_Beaufort_r2_299_b.csv → ACM6p5_Beaufort
EQSCENARIO_LIST=($(jq -r '.[].name | scan("(?<=s_lossesbyasset_).*(?=_r2)")' FINISHED.json))

# s_lossesbyasset_ACM6p5_Beaufort_r2_299_b.csv → ACM6p5_Beaufort_r2_299_b.csv
EQSCENARIO_LIST_LONGFORM=($(jq -r '.[].name | scan("(?<=s_lossesbyasset_).*r2.*\\.csv")' FINISHED.json))

echo -e "\n Importing scenario outputs into PostGIS..."
for eqscenario in ${EQSCENARIO_LIST[*]}
do
  RUN python3 DSRA_outputs2postgres_lfs.py --dsraModelDir=$DSRA_REPOSITORY --columnsINI=DSRA_outputs2postgres.ini --eqScenario=$eqscenario
done

echo "Importing Shakemap"
# Make a list of Shakemaps in the repo and download the raw csv files
DOWNLOAD_URL_LIST=($(jq -r '.[].url | scan(".*s_shakemap_.*\\.csv")' FINISHED.json))
for shakemap in ${DOWNLOAD_URL_LIST[*]}
do
    # Get the shakemap
    shakemap_filename=$( echo $shakemap | cut -f9- -d/ | cut -f1 -d?)
    RUN curl -H "Authorization: token ${GITHUB_TOKEN}" \
      --retry 999 --retry-max-time 0 \
      -o $shakemap_filename \
      -L $shakemap
    DOWNLOAD_URL=$(jq -r '.download_url' ${shakemap_filename})
      echo $DOWNLOAD_URL
    RUN curl -o $shakemap_filename \
      --retry 999 --retry-max-time 0 \
      -L $DOWNLOAD_URL
    # Run Create_table_shakemap.sql
    python3 DSRA_runCreateTableShakemap.py --shakemapFile=$shakemap_filename
done

# Run Create_table_shakemap_update.sql or Create_table_shakemap_update_ste.sql
SHAKEMAP_LIST=($(jq -r '.[].name | scan("s_shakemap_.*\\.csv")' FINISHED.json))
for ((i=0;i<${#EQSCENARIO_LIST_LONGFORM[@]};i++));
do
    item=${EQSCENARIO_LIST_LONGFORM[i]}
    #echo ${EQSCENARIO_LIST_LONGFORM[i]}
    #echo ${SHAKEMAP_LIST[i]}
    SITE=$(echo $item | cut -f5- -d_ | cut -c 1-1)
    eqscenario=$(echo $item | cut -f-2 -d_)
    #echo $eqscenario
    #echo $SITE
    if [ "$SITE" = "s" ]
    then
        echo "Site Model"
        RUN python3 DSRA_runCreateTableShakemapUpdate.py --eqScenario=$eqscenario --exposureAgg=$SITE
    elif [ "$SITE" = "b" ]
    then
        echo "Building Model"
        RUN python3 DSRA_runCreateTableShakemapUpdate.py --eqScenario=$eqscenario --exposureAgg=$SITE
    fi
    echo " "
done

echo -e "\n Importing Rupture Model"
python3 DSRA_ruptures2postgres.py --dsraRuptureDir="https://github.com/OpenDRR/scenario-catalogue/tree/master/deterministic/ruptures"

echo -e "\n Generating indicator views..."
for item in ${EQSCENARIO_LIST_LONGFORM[*]}
do
    SITE=$(echo $item | cut -f5- -d_ | cut -c 1-1)
    eqscenario=$(echo $item | cut -f-2 -d_)
    echo $eqscenario
    echo $SITE
    if [ "$SITE" = "s" ]
    then
        #echo "Site Model"
        RUN python3 DSRA_createRiskProfileIndicators.py --eqScenario=$eqscenario --aggregation=site_level --exposureModel=site
        RUN python3 DSRA_createRiskProfileIndicators.py --eqScenario=$eqscenario --aggregation=building --exposureModel=site
        RUN python3 DSRA_createRiskProfileIndicators.py --eqScenario=$eqscenario --aggregation=sauid  --exposureModel=site
    elif [ "$SITE" = "b" ]
    then
        #echo "Building Model"
        RUN python3 DSRA_createRiskProfileIndicators.py --eqScenario=$eqscenario --aggregation=building --exposureModel=building
        RUN python3 DSRA_createRiskProfileIndicators.py --eqScenario=$eqscenario --aggregation=sauid  --exposureModel=building
    fi
done

echo -e "\n Create Scenario Risk Master Tables at multiple aggregations"
RUN run_psql Create_scenario_risk_master_tables.sql

############################################################################################
#######################     Import Data from PostGIS to ElasticSearch   ####################
############################################################################################

if [[ ! -z "$ES_USER" ]]; then
  ES_CREDENTIALS="--user ${ES_USER}:${ES_PASS}"
fi

# Make sure Elasticsearch is ready prior to creating indexes
until $(curl -sSf -XGET --insecure ${ES_CREDENTIALS:-""} "${ES_ENDPOINT}/_cluster/health?wait_for_status=yellow" > /dev/null); do
    printf 'No status yellow from Elasticsearch, trying again in 10 seconds \n'
    sleep 10
done

# Load Probabilistic Model Indicators
if [ "$loadPsraModels" = true ]
then
    echo "Creating PSRA indices in ElasticSearch"
    for PT in ${PT_LIST[*]}
    do
      RUN python3 psra_postgres2es.py --province=$PT --dbview="all_indicators" --idField="building"
      RUN python3 psra_postgres2es.py --province=$PT --dbview="all_indicators" --idField="sauid"
      RUN python3 hmaps_postgres2es.py --province=$PT
      RUN python3 uhs_postgres2es.py --province=$PT
      RUN python3 srcLoss_postgres2es.py --province=$PT
    done

    echo "Creating PSRA Kibana Index Patterns"
    RUN curl -X POST -H "securitytenant: global" -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/api/saved_objects/index-pattern/psra*all_indicators_s" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"psra*all_indicators_s"}}'
    RUN curl -X POST -H "securitytenant: global" -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/api/saved_objects/index-pattern/psra*all_indicators_b" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"psra*all_indicators_b"}}'
    RUN curl -X POST -H "securitytenant: global" -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/api/saved_objects/index-pattern/psra_*_hmaps" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"psra_*_hmaps"}}'
    RUN curl -X POST -H "securitytenant: global" -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/api/saved_objects/index-pattern/psra_*_uhs" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"psra_*_uhs"}}'
    RUN curl -X POST -H "securitytenant: global" -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/api/saved_objects/index-pattern/psra_*_srcLoss" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"psra_*_srcLoss"}}'
fi

# Load Deterministic Model Indicators
if [ "$loadDsraScenario" = true ]
then
    for eqscenario in ${EQSCENARIO_LIST[*]}
    do
        echo -e "\nCreating elasticsearch indexes for DSRA..."
        RUN python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="all_indicators" --idField="building"
        RUN python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="all_indicators" --idField="sauid"
    done
fi


# Load Hazard Threat Views
if [ "$loadHazardThreat" = true ]
then
    # All Indicators
    RUN python3 hazardThreat_postgres2es.py  --type="all_indicators" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
fi


# Load physical exposure indicators
if [ "$loadPhysicalExposure" = true ]
then
    RUN python3 exposure_postgres2es.py --type="all_indicators" --aggregation="building" --geometry=geom_point --idField="BldgID"
    RUN python3 exposure_postgres2es.py --type="all_indicators" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
fi

# Load Risk Dynamics Views
if [ "$loadRiskDynamics" = true ]
then
    RUN python3 riskDynamics_postgres2es.py --type="all_indicators" --aggregation="sauid" --geometry=geom_point --idField="ghslID"
fi

# Load Social Fabric Views
if [ "$loadSocialFabric" = true ]
then
    RUN python3 socialFabric_postgres2es.py --type="all_indicators" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
fi


echo -e "\n Loading Kibana Saved Objects"
RUN curl -X POST -H "securitytenant: global" "${KIBANA_ENDPOINT}/api/saved_objects/_import" -H "kbn-xsrf: true" --form file=@kibanaSavedObjects.ndjson


# Restore PostgreSQL synchronous_commit default setting (on) for reliability
RUN set_synchronous_commit on

tail -f /dev/null & wait
