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

############################################################################################
############    Define global variables                                         ############
############################################################################################

# Needed variables (defined in .env for Docker Compose, or in a Amazon ECS task definition)
ENV_VAR_LIST=(
  POSTGRES_USER POSTGRES_PASS POSTGRES_PORT POSTGRES_HOST DB_NAME
  POPULATE_DB \
  KIBANA_ENDPOINT ES_ENDPOINT ES_USER ES_PASS \
  loadDsraScenario loadPsraModels loadHazardThreat loadPhysicalExposure
  loadRiskDynamics loadSocialFabric
)

ADD_DATA_PRINT_FUNCNAME=${ADD_DATA_PRINT_FUNCNAME:-true}
ADD_DATA_PRINT_LINENO=${ADD_DATA_PRINT_LINENO:-true}

DSRA_REPOSITORY=https://github.com/OpenDRR/earthquake-scenarios/tree/master/FINISHED

PT_LIST=(AB BC MB NB NL NS NT NU ON PE QC SK YT)

############################################################################################
############    Define helper and utility functions                             ############
############################################################################################

# Source: https://stackoverflow.com/questions/3685970/check-if-a-bash-array-contains-a-value
contains_element () {
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
}

# is_dry_run checks whether dry-run mode is enabled in .env of ECS task definition
is_dry_run() {
  [[ "${ADD_DATA_DRY_RUN,,}" =~ ^(true|1|y|yes|on)$ ]]
}

# LOG prints log message which hides secrets and preserves quoting
LOG() {
  local lineno
  local funcname

  # Print blank line before a new section
  [[ $# == 1 ]] && [[ "$1" =~ ^#{1,2}[[:space:]] ]] && echo

  if [[ "${ADD_DATA_PRINT_LINENO,,}" =~ ^(true|1|y|yes|on)$ ]] || \
     [[ "${ADD_DATA_PRINT_FUNCNAME,,}" =~ ^(true|1|y|yes|on)$ ]]
  then
    local i=0
    while [[ "${FUNCNAME[i]}" =~ ^(LOG|RUN|INFO|WARN|ERROR)$ ]]; do
      (( i += 1 ))
    done
    lineno=:${BASH_LINENO[i-1]}
    funcname=:${FUNCNAME[i]}
  fi
  echo -n "[add_data$lineno$funcname]"

  for i in "$@"; do
    # Hide secrets
    i="${i//$GITHUB_TOKEN/***}"
    [[ ${POSTGRES_PASS,,} != password ]] && i="${i//$POSTGRES_PASS/***}"
    [[ ${ES_PASS,,} != password ]] && i="${i//$ES_PASS/***}"
    # Try to add quotes as appropriate
    if [[ $# -gt 1 ]] && echo "$i" | grep -q ' '; then
      if echo "$i" | grep -q "'"; then
        i="\"$i\""
      else
        i="'$i'"
      fi
    fi
    echo -n " $i"
  done
  echo
}

# INFO logs an informative message
INFO() {
  LOG "INFO: $*"
}

# WARN logs a warning message
WARN() {
  LOG "WARNING: $*"
}

# ERROR logs an error message and exits this program
ERROR() {
  LOG "ERROR: $*"
  exit 1
}

# RUN runs a command, logs, and prints timing and memory information
RUN() {
  if is_dry_run && [[ -n $(type -p "$1") ]]; then
    LOG DRY_RUN: "$@"
    return
  fi

  LOG RUN: "$@"
  if [[ -x /usr/bin/time ]] && [[ -n $(type -p "$1") ]]; then
    # file
    time /usr/bin/time "$@"
  else
    # alias, keyword, function, or builtin
    if is_dry_run; then
      "$@"
    else
      time "$@"
    fi
  fi
}

# set_synchronous_commit sets database's synchronous_commit
# to "off" for speed, or to "on" for reliability.
set_synchronous_commit() {
  if [ "$#" -ne 1 ]; then
    ERROR "${FUNCNAME[0]} requires exactly one argument, but $# was given."
  fi
  local on_off="$1"

  LOG "psql: Setting synchronous_commit TO $on_off..."
  RUN psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" -a \
    -c "SHOW synchronous_commit;"
  RUN psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" -a \
    -c "ALTER DATABASE $DB_NAME SET synchronous_commit TO $on_off;"
  RUN psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" -a \
    -c "SHOW synchronous_commit;"
}

# run_ogr2ogr creates boundaries schema geometry tables from default geopackages.
# (Change ogr2ogr PATH / geopackage path if necessary to run.)
run_ogr2ogr() {
  if [ "$#" -ne 1 ]; then
    ERROR "${FUNCNAME[0]} requires exactly one argument, but $# was given."
  fi
  local id="$1"

  local srs_def="EPSG:4326"
  local dst_datasource_name=PG:"host='$POSTGRES_HOST' user='$POSTGRES_USER' dbname='$DB_NAME' password='$POSTGRES_PASS'"
  local src_datasource_name="boundaries/$id.gpkg"
  local nln
  nln="boundaries.$(basename "$id")"

  LOG "ogr2ogr: Importing $src_datasource_name into $DB_NAME..."

  RUN ogr2ogr -t_srs "$srs_def" \
	  -f PostgreSQL \
	  "$dst_datasource_name" \
	  "$src_datasource_name" \
	  -lco LAUNDER=NO \
	  -nln "$nln"
}

# run_psql runs PostgreSQL queries from a given input SQL file.
run_psql() {
  if [ "$#" -ne 1 ]; then
    ERROR "${FUNCNAME[0]} requires exactly one argument, but $# was given."
  fi
  local input_file="$1"

  RUN psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" -a -f "$input_file"
}

# fetch_csv_lfs downloads CSV data files from OpenDRR repos
# with help from GitHub API with support for LFS files.
# See https://docs.github.com/en/rest/reference/repos#get-repository-content
fetch_csv_lfs() {
  if [ "$#" -ne 2 ]; then
    ERROR "${FUNCNAME[0]} requires exactly two arguments, but $# was given."
  fi
  local owner="OpenDRR"
  local repo="$1"
  local path="$2"
  local output_file
  local response="github-api/$2.json"
  local download_url
  local size

  output_file=$(basename "$path" | sed -e 's/?.*//')

  mkdir -p github-api/"$(dirname "$path")"

  INFO "$repo/$path"
  RUN curl -s -o "$response" \
    --retry 999 --retry-max-time 0 \
    -H "Authorization: token ${GITHUB_TOKEN}" \
    -L "https://api.github.com/repos/$owner/$repo/contents/$path"

  is_dry_run || download_url=$(jq -r '.download_url' "$response")
  is_dry_run || size=$(jq -r '.size' "$response")

  # TODO: Actually use these values for verification
  echo download_url="$download_url"
  echo size="$size"

  LOG "Download from $download_url"
  RUN curl -o "$output_file" -L "$download_url" --retry 999 --retry-max-time 0
}

# fetch_csv_xz downloads CSV data files from OpenDRR xz-compressed repos
fetch_csv_xz() {
  if [ "$#" -ne 2 ]; then
    ERROR "${FUNCNAME[0]} requires exactly two arguments, but $# was given."
  fi
  local owner="OpenDRR"
  local repo="$1"
  local path="$2"
  local output_file
  local response
  local path_dir
  local download_url

  output_file=$(basename "$path" | sed -e 's/?.*//')
  path_dir=$(dirname "$path")

  INFO "$path_dir"

  # Fetch directory listing
  RUN mkdir -p "github-api/$path_dir"
  response="github-api/$path_dir.dir.json"
  RUN curl -s -o "$response" \
    --retry 999 --retry-max-time 0 \
    -H "Authorization: token ${GITHUB_TOKEN}" \
    -H "Accept: application/vnd.github.v3+json" \
    -L "https://api.github.com/repos/$owner/$repo-xz/contents/$path_dir"

  is_dry_run || download_url=$(jq -r '.[] | select(.name == "'"$output_file"'.xz") | .download_url' "$response")
  LOG "${FUNCNAME[0]}: Download from $download_url"
  RUN curl -o "$output_file.xz" -L "$download_url" --retry 999 --retry-max-time 0

  # TODO: Keep the compressed file somewhere, uncompress when needed
  RUN unxz "$output_file.xz"
}

# fetch_csv calls either fetch_csv_xz or fetch_csv_lfs to fetch CSV files
fetch_csv() {
  # TODO: Make it more intelligent.
  RUN fetch_csv_xz "$@" || RUN fetch_csv_lfs "$@"
}

# fetch_psra_csv_from_model fetches CSV files from the specified model
# for all provinces and territories.
# NOTE: Right now, this function strips the OpenQuake comment header too,
#       but this may change when the support for downloading compressed CSV
#       is added.
fetch_psra_csv_from_model() {
  if [ "$#" != "1" ]; then
    ERROR "${FUNCNAME[0]} requires exactly one argument, but $# was given."
  fi

  model=$1

  for PT in "${PT_LIST[@]}"; do
    RUN curl -H "Authorization: token ${GITHUB_TOKEN}" \
      --retry 999 --retry-max-time 0 \
      -o "${PT}.json" \
      -L "https://api.github.com/repos/OpenDRR/canada-srm2/contents/$model/output/$PT"

    RUN mapfile -t DOWNLOAD_LIST < <(jq -r '.[].url | select(. | contains(".csv"))' "${PT}.json")

    mkdir -p "$model/$PT"
    ( cd "$model/$PT"
      for file in "${DOWNLOAD_LIST[@]}"; do
        FILENAME=$(echo "$file" | cut -f-1 -d? | cut -f11- -d/)
        RUN curl -H "Authorization: token ${GITHUB_TOKEN}" \
          --retry 999 --retry-max-time 0 \
          -o "$FILENAME" \
          -L "$file"
        is_dry_run || DOWNLOAD_URL=$(jq -r '.download_url' "$FILENAME")
        RUN curl -o "$FILENAME" \
          --retry 999 --retry-max-time 0 \
          -L "$DOWNLOAD_URL"

        # Strip OpenQuake comment header if exists
        # (safe for cH_${PT}_hmaps_xref.csv)
        RUN sed -i -r $'1{/^(\xEF\xBB\xBF)?#,/d}' "$FILENAME"
      done
      # TODO: Use a different for ${PT}.json, and keep for debugging
      RUN rm -f "${PT}.json"
    )
  done
}

# merge_csv merges CSV files without repeating column headers.
# Syntax: merge_cvs [INPUT_CSV_FILES]... [OUTPUT_FILE]
# NOTE: The '#,,,,,"generated_by='OpenQuake engine 3.x..."' comment header
#       is currently NOT removed removed by merge_csv()
#       but by fetch_psra_csv_from_model().  This may change in the future.
merge_csv() {
  if [ "$#" -lt "2" ]; then
    ERROR "${FUNCNAME[0]} requires at least two arguments, but $# was given."
  fi
  local input_files=("${@:1:$#-1}")
  local output_file="${*:$#}"

  INFO "merge_cvs input: ${input_files[*]}"
  INFO "merge_cvs output: $output_file"

  if [[ $# = 2 ]] && [[ $1 = "$2" ]]; then
    INFO "There is only one input file, and it has the same name as output file, skipping."
    return
  fi

  if contains_element "$output_file" "${input_files[@]}"; then
    ERROR "Output file \"$output_file\" is listed among input files: \"${input_files[*]}\""
  fi

  if [ -e "$output_file" ]; then
    WARN "Output file \"$output_file\" already exists!  Overwriting..."
  fi

  # The "awk" magic that merge CSV files while stripping duplicated headers.
  # See https://apple.stackexchange.com/questions/80611/merging-multiple-csv-files-without-merging-the-header
  # NOTE: DO NOT prepend RUN to the following awk command, as otherwise
  #       the log would be the first line in the merged CSV file!
  #       See reviews at #105 for more information.
  is_dry_run || awk '(NR == 1) || (FNR > 1)' "${input_files[@]}" > "$output_file"
}

############################################################################################
############    Define "Set up job" functions                                   ############
############################################################################################

# setup_eatmydata preloads libeatmydata to transparently disable fsync()
# and other data-to-disk synchronization calls for potential speed up
setup_eatmydata() {
  LD_LIBRARY_PATH=${LD_LIBRARY_PATH:+"$LD_LIBRARY_PATH:"}/usr/lib/libeatmydata
  LD_PRELOAD=${LD_PRELOAD:+"$LD_PRELOAD "}libeatmydata.so
  export LD_LIBRARY_PATH LD_PRELOAD
}

# check_environment_variables ensures required environment variables are defined,
# either in Docker Compose .env file, or in Amazon ECS task definition.
check_environment_variables() {
  LOG "## Check needed environment variables"
  for var in "${ENV_VAR_LIST[@]}"; do
    INFO "$var = ${!var}"
    if [[ -z ${!var} ]]; then
      if [[ "$var" =~ ^(ES_USER|ES_PASS)$ ]]; then
        WARN "Optional variable $var is not set.  Continuing..."
      else
        ERROR "Mandatory variable $var is not set!  Aborting..."
      fi
    fi
  done
}

# read_github_token reads GitHub personal access token from config file
# and checks for its validity
read_github_token() {
  LOG "## Read GitHub token from config.ini"
  # See https://github.blog/changelog/2021-03-31-authentication-token-format-updates-are-generally-available/
  GITHUB_TOKEN=$(sed -n -r 's/^\s*github_token\s*=\s*([A-Za-z0-9_]+).*/\1/p' config.ini | tail -1)
  INFO "GITHUB_TOKEN is ${#GITHUB_TOKEN} characters in length"

  if [[ ${#GITHUB_TOKEN} -lt 40 ]]; then
    WARN "Your GITHUB_TOKEN has a length of ${#GITHUB_TOKEN} characters, but 40 or above is expected."
  fi

  tmpfile=$(mktemp)
  status_code=$(curl --write-out "%{http_code}" --silent --output "$tmpfile" \
    -H "Authorization: token ${GITHUB_TOKEN}" \
    -L https://api.github.com/repos/OpenDRR/canada-srm2/contents/cDamage/output)
  INFO "Access to OpenDRR/canada-srm2 returns HTTP status code $status_code"

  if [[ "$status_code" -ne 200 ]] ; then
    cat "$tmpfile"
    case "$status_code" in
      401)
        ERROR "Your GitHub token is invalid or has expired. Aborting..."
        ;;
      404)
        ERROR "Your GitHub token was unable to access https://github.com/OpenDRR/canada-srm2. Please ensure the \"repo\" scope is enabled for the token. Aborting..."
        ;;
      *)
        ERROR "Unhandled error ($status_code): Please try again. Aborting..."
        ;;
    esac
  fi
}

# get_model-factory scripts downloads essential Python and SQL scripts
# from the OpenDRR/model-factory repository
get_model_factory_scripts() {
  # TODO: Make this more robust
  RUN git clone https://github.com/OpenDRR/model-factory.git --depth 1 || (cd model-factory ; RUN git pull)

  # Copy model-factory scripts to working directory
  # TODO: Find ways to keep these scripts in their place without copying them all to WORKDIR
  RUN cp model-factory/scripts/*.* .
  #rm -rf model-factory
}

# get_git_lfs_pointers_of_csv_files fetches from source data repositories
# Git LFS pointers of CSV files for "oid sha256" values that are used for
# checksum verification later in the program.
get_git_lfs_pointers_of_csv_files() {
  LOG '## Fetch Git LFS pointers of CSV files for "oid sha256"'
  local base_dir=git-sha256
  rm -rf "$base_dir"
  mkdir -p "$base_dir"
  ( cd "$base_dir" && \
    for repo in canada-srm2 model-inputs openquake-inputs earthquake-scenarios; do
      RUN git clone --filter=blob:none --no-checkout "https://${GITHUB_TOKEN}@github.com/OpenDRR/${repo}.git"
      is_dry_run || \
        ( cd $repo && \
          git sparse-checkout set '*.csv' && \
          GIT_LFS_SKIP_SMUDGE=1 git checkout )
    done
  )
}

# wait_for_postgres waits until PostGIS is ready to accept connections
wait_for_postgres() {
  LOG "Wait until PostgreSQL is ready"
  # NOTE: Replacing '-p 5432' with '-p "$POSTGRES_PORT"' does not work
  # See the reviews in #105 for more information.
  # TODO: Investigate and document, and resolve further if possible.
  until RUN pg_isready -h "$POSTGRES_HOST" -p 5432 -U "$POSTGRES_USER"; do
    sleep 2
  done
}

############################################################################################
############    Define "Process Exposure and Ancillary Data" functions          ############
############################################################################################

import_census_boundaries() {
  LOG "## Importing Census Boundaries"

  INFO "Trying to download pre-generated PostGIS database dump (for speed)..."
  if RUN curl -O -v --retry 999 --retry-max-time 0 https://opendrr.eccp.ca/file/OpenDRR/opendrr-boundaries.dump || \
    RUN curl -O -v --retry 999 --retry-max-time 0 https://f000.backblazeb2.com/file/OpenDRR/opendrr-boundaries.dump
  then
    RUN pg_restore -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" \
      --verbose --clean --if-exists --create opendrr-boundaries.dump \
      | while IFS= read -r line; do printf '%s %s\n' "$(date "+%Y-%m-%d %H:%M:%S")" "$line"; done
  else
    WARN "Unable to fetch opendrr-boundaries.dump."
    WARN "Fallback to fetching boundaries CSV files via Git LFS:"
    RUN git clone https://github.com/OpenDRR/boundaries.git --depth 1 || (cd boundaries ; RUN git pull)

    # Create boundaries schema geometry tables from default geopackages.
    for i in ADAUID CANADA CDUID CSDUID DAUID ERUID FSAUID PRUID SAUID; do
      RUN run_ogr2ogr "Geometry_$i"
    done

    for i in HexGrid_5km HexGrid_10km HexGrid_25km HexGrid_50km HexGrid_GlobalFabric SAUID_HexGrid SAUID_HexGrid_5km_intersect SAUID_HexGrid_10km_intersect SAUID_HexGrid_25km_intersect SAUID_HexGrid_50km_intersect SAUID_HexGrid_100km_intersect SAUID_HexGrid_GlobalFabric_intersect; do
      RUN run_ogr2ogr "hexbin_4326/$i"
    done

    #rm -rf boundaries
  fi

  RUN run_psql Update_boundaries_SAUID_table.sql
}

import_physical_exposure_model() {
  # Physical Exposure
  LOG "## Importing Physical Exposure Model into PostGIS"

  RUN fetch_csv openquake-inputs \
    exposure/general-building-stock/BldgExpRef_CA_master_v3p2.csv
  RUN run_psql Create_table_canada_exposure.sql

  RUN fetch_csv model-inputs \
    exposure/building-inventory/metro-vancouver/PhysExpRef_MetroVan_v4.csv
  RUN run_psql Create_table_canada_site_exposure_ste.sql
}

import_vs30_model() {
  # VS30
  LOG "## Importing VS30 Model into PostGIS"

  RUN fetch_csv openquake-inputs \
    earthquake/sites/regions/vs30_CAN_site_model_xref.csv

  RUN fetch_csv openquake-inputs \
    earthquake/sites/regions/site-vgrid_CA.csv

  #Correct CRLF 
  sed -i 's/\r//g' /usr/src/app/site-vgrid_CA.csv

  RUN run_psql Create_table_vs_30_CAN_site_model.sql
  RUN run_psql Create_table_vs_30_CAN_site_model_xref.sql
}

import_census_data() {
  # Census Data
  LOG "## Importing Census Data"

  RUN fetch_csv openquake-inputs \
    exposure/census-ref-sauid/census-attributes-2016.csv
  RUN run_psql Create_table_2016_census_v3.sql
}

import_sovi() {
  LOG "## Importing Sovi"
  # Need to source tables
  RUN fetch_csv openquake-inputs \
    social-vulnerability/social-vulnerability-census.csv
  RUN fetch_csv openquake-inputs \
    social-vulnerability/social-vulnerability-index.csv
  RUN fetch_csv openquake-inputs \
    social-vulnerability/sovi_thresholds_2021.csv

  RUN run_psql Create_table_sovi_index_canada_v2.sql
  RUN run_psql Create_table_sovi_census_canada.sql
  RUN run_psql Create_table_sovi_thresholds.sql
}

import_luts() {
  LOG "## Importing LUTs"
  RUN fetch_csv openquake-inputs \
    exposure/general-building-stock/1.%20documentation/collapse_probability.csv
  RUN run_psql Create_collapse_probability_table.sql
}

import_retrofit_costs() {
  LOG "## Retrofit Costs"
  RUN fetch_csv openquake-inputs \
    exposure/general-building-stock/1. documentation/retrofit_costs.csv
  RUN run_psql Create_retrofit_costs_table.sql
}

import_ghsl() {
  LOG "## Importing GHSL"
  RUN fetch_csv openquake-inputs \
    natural-hazards/mh-intensity-ghsl.csv
  RUN run_psql Create_table_GHSL.sql
}

import_mh_intensity() {
  LOG "## Importing MH Intensity"
  RUN fetch_csv openquake-inputs \
    natural-hazards/HTi_sauid.csv
}

import_hazard_threat_thresholds() {
  LOG "## Importing Hazard Threat Thresholds"
  RUN fetch_csv openquake-inputs \
    natural-hazards/HTi_thresholds.csv
  RUN fetch_csv openquake-inputs \
    natural-hazards/hazard_threat_rating_thresholds.csv
}

post_process_mh_tables() {
  RUN run_psql Create_table_mh_intensity_canada_v2.sql
  RUN run_psql Create_table_mh_thresholds.sql
  RUN run_psql Create_table_mh_rating_thresholds.sql
  # RUN run_psql Create_MH_risk_sauid_prioritization_prereq_tables.sql
  # RUN run_psql Create_MH_risk_sauid_prioritization_Canada.sql
  # RUN run_psql Create_MH_risk_sauid_ALL.sql
}

copy_ancillary_tables() {
  LOG '## Use python to run \copy from a system call'
  RUN python3 copyAncillaryTables.py
}

post_process_all_tables_update() {
  LOG "## Perform update operations on all tables after data copied into tables"
  RUN run_psql Create_all_tables_update.sql
  RUN run_psql Create_site_exposure_to_building_and_sauid.sql
  RUN run_psql Create_table_vs_30_BC_CAN_model_update_site_exposure.sql
}

generate_indicators() {
  LOG "## Generate Indicators"
  RUN run_psql Create_physical_exposure_building_indicators_PhysicalExposure.sql
  RUN run_psql Create_physical_exposure_sauid_indicators_view_PhysicalExposure.sql
  RUN run_psql Create_physical_exposure_building_indicators_PhysicalExposure_ste.sql
  RUN run_psql Create_physical_exposure_sauid_indicators_view_PhysicalExposure_ste.sql
  RUN run_psql Create_physical_exposure_site_level_indicators_PhysicalExposure_ste.sql
  RUN run_psql Create_risk_dynamics_indicators.sql
  RUN run_psql Create_social_vulnerability_sauid_indicators_SocialFabric.sql
  RUN run_psql Create_MH_risk_sauid_prioritization_prereq_tables.sql
  RUN run_psql Create_MH_risk_sauid_prioritization_Canada.sql
  # RUN run_psql Create_MH_risk_sauid_ALL.sql
  RUN run_psql Create_hexbin_physical_exposure_aggregation_area_proxy.sql
  # RUN run_psql Create_hexbin_physical_exposure_hexbin_aggregation_centroid.sql
  RUN run_psql Create_hexbin_MH_risk_sauid_prioritization_aggregation_area.sql
  # RUN run_psql Create_hexbin_MH_risk_sauid_prioritization_aggregation_centroid.sql
  RUN run_psql Create_hexbin_social_vulnerability_aggregation_area_proxy.sql
  # RUN run_psql Create_hexbin_social_vulnerability_aggregation_centroid.sql
}

############################################################################################
############    Define "Process PSRA" functions                                 ############
############################################################################################

import_raw_psra_tables() {
  LOG "## Importing Raw PSRA Tables"

  LOG "### Get list of provinces & territories"
  RUN curl -H "Authorization: token ${GITHUB_TOKEN}" \
    --retry 999 --retry-max-time 0 \
    -o output.json \
    -L https://api.github.com/repos/OpenDRR/canada-srm2/contents/cDamage/output

  # TODO: Compare PT_LIST with FETCHED_PT_LIST
  RUN mapfile -t FETCHED_PT_LIST < <(jq -r '.[].name' output.json)

  LOG "### cDamage"
  RUN fetch_psra_csv_from_model cDamage

  for PT in "${PT_LIST[@]}"; do
    ( cd "cDamage/$PT"
      RUN merge_csv cD_*dmg-mean_b0.csv "cD_${PT}_dmg-mean_b0.csv"
      RUN merge_csv cD_*dmg-mean_r2.csv "cD_${PT}_dmg-mean_r2.csv"
    )
  done

  LOG "### cHazard"
  RUN fetch_psra_csv_from_model cHazard

  # This was only needed when the cHazard data was divided by economic region
  # for PT in "${PT_LIST[@]}"; do
  #   ( cd "cHazard/$PT"
  #     RUN python3 /usr/src/app/PSRA_hCurveTableCombine.py --hCurveDir="/usr/src/app/cHazard/$PT/"
  #   )
  # done

  LOG "### eDamage"
  RUN fetch_psra_csv_from_model eDamage

  for PT in "${PT_LIST[@]}"; do
    ( cd "eDamage/$PT"
      RUN merge_csv eD_*damages-mean_b0.csv "eD_${PT}_damages-mean_b0.csv"
      RUN merge_csv eD_*damages-mean_r2.csv "eD_${PT}_damages-mean_r2.csv"
    )
  done

  LOG "### ebRisk"
  RUN fetch_psra_csv_from_model ebRisk

  for PT in "${PT_LIST[@]}"; do
    ( cd "ebRisk/$PT"
      RUN merge_csv ebR_*agg_curves-stats_b0.csv "ebR_${PT}_agg_curves-stats_b0.csv"
      RUN merge_csv ebR_*agg_curves-stats_r2.csv "ebR_${PT}_agg_curves-stats_r2.csv"
      RUN merge_csv ebR_*agg_losses-stats_b0.csv "ebR_${PT}_agg_losses-stats_b0.csv"
      RUN merge_csv ebR_*agg_losses-stats_r2.csv "ebR_${PT}_agg_losses-stats_r2.csv"
      RUN merge_csv ebR_*avg_losses-stats_b0.csv "ebR_${PT}_avg_losses-stats_b0.csv"
      RUN merge_csv ebR_*avg_losses-stats_r2.csv "ebR_${PT}_avg_losses-stats_r2.csv"

      # Combine source loss tables for runs that were split by economic region or sub-region
      RUN python3 /usr/src/app/PSRA_combineSrcLossTable.py --srcLossDir="/usr/src/app/ebRisk/$PT"
    )
  done
}

post_process_psra_tables() {
  LOG "## PSRA_0"
  RUN run_psql psra_0.create_psra_schema.sql

  LOG "## PSRA_1-8"
  for PT in "${PT_LIST[@]}"; do
    RUN python3 PSRA_runCreate_tables.py --province="$PT" --sqlScript="psra_1.Create_tables.sql"
    RUN python3 PSRA_copyTables.py --province="$PT"
    RUN python3 PSRA_sqlWrapper.py --province="$PT" --sqlScript="psra_2.Create_table_updates.sql"
    RUN python3 PSRA_sqlWrapper.py --province="$PT" --sqlScript="psra_3.Create_psra_building_all_indicators.sql"
    RUN python3 PSRA_sqlWrapper.py --province="$PT" --sqlScript="psra_4.Create_psra_sauid_all_indicators.sql"
    RUN python3 PSRA_sqlWrapper.py --province="$PT" --sqlScript="psra_5.Create_psra_sauid_references_indicators.sql"
  done

  RUN run_psql psra_6.Create_psra_merge_into_national_indicators.sql
}

############################################################################################
############    Define "Process DSRA" functions                                 ############
############################################################################################

import_earthquake_scenarios() {
  LOG "## Get list of earthquake scenarios"
  RUN curl -H "Authorization: token ${GITHUB_TOKEN}" \
    --retry 999 --retry-max-time 0 \
    -o FINISHED.json \
    -L https://api.github.com/repos/OpenDRR/earthquake-scenarios/contents/FINISHED

  # s_lossesbyasset_ACM6p5_Beaufort_r1_299_b.csv → ACM6p5_Beaufort
  RUN mapfile -t EQSCENARIO_LIST < <(jq -r '.[].name | scan("(?<=s_lossesbyasset_).*(?=_r1)")' FINISHED.json)

  # s_lossesbyasset_ACM6p5_Beaufort_r1_299_b.csv → ACM6p5_Beaufort_r1_299_b.csv
  RUN mapfile -t EQSCENARIO_LIST_LONGFORM < <(jq -r '.[].name | scan("(?<=s_lossesbyasset_).*r1.*\\.csv")' FINISHED.json)

  LOG "## Importing scenario outputs into PostGIS"
  for eqscenario in ${EQSCENARIO_LIST[*]}; do
    RUN python3 DSRA_outputs2postgres_lfs.py --dsraModelDir=$DSRA_REPOSITORY --columnsINI=DSRA_outputs2postgres.ini --eqScenario="$eqscenario"
  done
}

import_shakemap() {
  LOG "## Importing Shakemap"
  # Make a list of Shakemaps in the repo and download the raw csv files
  mapfile -t DOWNLOAD_URL_LIST < <(jq -r '.[].url | scan(".*s_shakemap_.*\\.csv")' FINISHED.json)
  for shakemap in ${DOWNLOAD_URL_LIST[*]}; do
    # Get the shakemap
    shakemap_filename=$( echo "$shakemap" | cut -f9- -d/ | cut -f1 -d?)
    RUN curl -H "Authorization: token ${GITHUB_TOKEN}" \
      --retry 999 --retry-max-time 0 \
      -o "$shakemap_filename" \
      -L "$shakemap"
    is_dry_run || DOWNLOAD_URL=$(jq -r '.download_url' "$shakemap_filename")
    LOG "$DOWNLOAD_URL"
    RUN curl -o "$shakemap_filename" \
      --retry 999 --retry-max-time 0 \
      -L "$DOWNLOAD_URL"

    # Run Create_table_shakemap.sql
    RUN python3 DSRA_runCreateTableShakemap.py --shakemapFile="$shakemap_filename"
  done

  # Run Create_table_shakemap_update.sql or Create_table_shakemap_update_ste.sql
  RUN mapfile -t SHAKEMAP_LIST < <(jq -r '.[].name | scan("s_shakemap_.*\\.csv")' FINISHED.json)
  for ((i=0;i<${#EQSCENARIO_LIST_LONGFORM[@]};i++)); do
    item=${EQSCENARIO_LIST_LONGFORM[i]}
    #echo ${EQSCENARIO_LIST_LONGFORM[i]}
    #echo ${SHAKEMAP_LIST[i]}
    SITE=$(echo "$item" | cut -f5- -d_ | cut -c 1-1)
    eqscenario=$(echo "$item" | cut -f-2 -d_)
    #echo $eqscenario
    #echo $SITE
    case $SITE in
      s)
        echo "Site Model"
        RUN python3 DSRA_runCreateTableShakemapUpdate.py --eqScenario="$eqscenario" --exposureAgg="$SITE"
        ;;
      b)
        echo "Building Model"
        RUN python3 DSRA_runCreateTableShakemapUpdate.py --eqScenario="$eqscenario" --exposureAgg="$SITE"
        ;;
    esac
    echo " "    # TODO: Find out the purpose of this echo statement
  done
}

import_rupture_model() {
LOG "## Importing Rupture Model"
RUN python3 DSRA_ruptures2postgres.py --dsraRuptureDir="https://github.com/OpenDRR/earthquake-scenarios/tree/master/ruptures"

LOG "## Generating indicator views"
  for item in ${EQSCENARIO_LIST_LONGFORM[*]}; do
    SITE=$(echo "$item" | cut -f5- -d_ | cut -c 1-1)
    eqscenario=$(echo "$item" | cut -f-2 -d_)
    echo "$eqscenario"
    echo "$SITE"
    case $SITE in
      s)
        #echo "Site Model"
        RUN python3 DSRA_createRiskProfileIndicators.py --eqScenario="$eqscenario" --aggregation=site_level --exposureModel=site
        RUN python3 DSRA_createRiskProfileIndicators.py --eqScenario="$eqscenario" --aggregation=building --exposureModel=site
        RUN python3 DSRA_createRiskProfileIndicators.py --eqScenario="$eqscenario" --aggregation=sauid  --exposureModel=site
        ;;
      b)
        #echo "Building Model"
        RUN python3 DSRA_createRiskProfileIndicators.py --eqScenario="$eqscenario" --aggregation=building --exposureModel=building
        RUN python3 DSRA_createRiskProfileIndicators.py --eqScenario="$eqscenario" --aggregation=sauid  --exposureModel=building
        ;;
    esac
  done
}

create_scenario_risk_master_tables() {
  LOG "## Create Scenario Risk Master Tables at multiple aggregations"
  RUN run_psql Create_scenario_risk_master_tables.sql
}

############################################################################################
############    Define "Import Data from PostGIS to Elasticsearch" functions    ############
############################################################################################

export_to_elasticsearch() {
  if [[ -n $ES_USER ]]; then
    ES_CREDENTIALS="--user ${ES_USER}:${ES_PASS}"
  fi

  LOG "## Make sure Elasticsearch is ready prior to creating indexes"
  # shellcheck disable=SC2086
  until RUN curl -sSf -XGET --insecure ${ES_CREDENTIALS:-} "${ES_ENDPOINT}/_cluster/health?wait_for_status=yellow"; do
    LOG "No status yellow from Elasticsearch, trying again in 10 seconds"
    sleep 10
  done

  LOG "## Create Kibana Space"
  RUN curl  -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/api/spaces/space"  -H "kbn-xsrf: true" -d '{"id": "gsc-cgc","name": "GSC-CGC","description" : "Geological Survey of Canada Private Space","color": "#aabbcc","initials": "G"}'

  LOG "## Load Probabilistic Model Indicators"
  # shellcheck disable=SC2154
  if [ "$loadPsraModels" = true ]; then
    LOG "Creating PSRA indices in Elasticsearch"
    RUN python3 psra_postgres2es.py
    RUN python3 hmaps_postgres2es.py
    RUN python3 uhs_postgres2es.py
    RUN python3 srcLoss_postgres2es.py

    LOG "Creating PSRA Kibana Index Patterns"
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_psra_indicators_s" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_psra_indicators_s"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_psra_indicators_b" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_psra_indicators_b"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_psra_hmaps" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_psra_hmaps"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_psra_uhs" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_psra_uhs"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_psra_srcLoss" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_psra_srcLoss"}}'
  fi

  # Load Deterministic Model Indicators
  # shellcheck disable=SC2154
  if [[ "$loadDsraScenario" = true ]]; then
for eqscenario in ${EQSCENARIO_LIST[*]}; do
  LOG "Creating Elasticsearch indexes for DSRA"
  RUN python3 dsra_postgres2es.py --eqScenario="$eqscenario" --dbview="all_indicators" --idField="building"
  RUN python3 dsra_postgres2es.py --eqScenario="$eqscenario" --dbview="all_indicators" --idField="sauid"

  # LOG "Creating DSRA Kibana Index Patterns"
  # Need to develop saved object workflow for automated index patern generation
  # RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_dsra_${eqscenario}_all_indicators_s" -H "kbn-xsrf: true" -d "{ 'attributes': { 'title':'opendrr_dsra_${eqscenario}_all_indicators_s'}}"
  # RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_dsra_${eqscenario}_all_indicators_b" -H "kbn-xsrf: true" -d "{ 'attributes': { 'title':'opendrr_dsra_${eqscenario}_all_indicators_b'}}"
done
  fi

  # Load Hazard Threat Views
  # shellcheck disable=SC2154
  # 2021/09/21 DR - Keeping Hazard Threah and Risk Dynamics out of ES for the time being
  # if [[ "$loadHazardThreat" = true ]]; then
  #   # All Indicators
  #   LOG "Creating Elasticsearch indexes for Hazard Threat"
  #   RUN python3 hazardThreat_postgres2es.py  --type="all_indicators" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"

  #   LOG "Creating Hazard Threat Kibana Index Patterns"
  #   RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hazard_threat_all_indicators_s" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hazard_threat_all_indicators_s"}}'
  # fi

  # Load physical exposure indicators
  # shellcheck disable=SC2154
  if [[ $loadPhysicalExposure = true ]]; then
    LOG "Creating Elasticsearch indexes for Physical Exposure"
    RUN python3 exposure_postgres2es.py --aggregation="building" --geometry=geom_point
    RUN python3 exposure_postgres2es.py --aggregation="sauid" --geometry=geom_poly

    LOG "Creating Exposure Kibana Index Patterns"
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_physical_exposure_all_indicators_s" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_physical_exposure_all_indicators_s"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_physical_exposure_all_indicators_b" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_physical_exposure_all_indicators_b"}}'
  fi

  # Load Risk Dynamics Views
  # shellcheck disable=SC2154
  # 2021/09/21 DR - Keeping Hazard Threah and Risk Dynamics out of ES for the time being
  # if [[ $loadRiskDynamics = true ]]; then
  #   LOG "Creating Elasticsearch indexes for Risk Dynamics"
  #   RUN python3 riskDynamics_postgres2es.py --type="all_indicators" --aggregation="sauid" --geometry=geom_point --idField="ghslID"

  #   LOG "Creating Risk Dynamics Kibana Index Patterns"
  #   RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_risk_dynamics_all_indicators" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_risk_dynamics_all_indicators"}}'
  # fi

  # Load Social Fabric Views
  # shellcheck disable=SC2154
  if [[ $loadSocialFabric = true ]]; then
    LOG "Creating Elasticsearch indexes for Social Fabric"
    RUN python3 socialFabric_postgres2es.py --aggregation="sauid" --geometry=geom_poly --sortfield="Sauid"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_5km" --geometry=geom --sortfield="gridid_5"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_10km" --geometry=geom --sortfield="gridid_10"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_25km" --geometry=geom --sortfield="gridid_25"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_50km" --geometry=geom --sortfield="gridid_50"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_100km" --geometry=geom --sortfield="gridid_100"

    LOG "Creating Social Fabric Kibana Index Patterns"
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_social_fabric_all_indicators_s" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_social_fabric_all_indicators_s"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_social_fabric_all_indicators_hexgrid_5km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_social_fabric_all_indicators_hexgrid_5km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_social_fabric_all_indicators_hexgrid_10km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_social_fabric_all_indicators_hexgrid_10km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_social_fabric_all_indicators_hexgrid_25km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_social_fabric_all_indicators_hexgrid_25km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_social_fabric_all_indicators_hexgrid_50km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_social_fabric_all_indicators_hexgrid_50km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_social_fabric_all_indicators_hexgrid_100km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_social_fabric_all_indicators_hexgrid_100km"}}'
  fi

  # Load Hexgrid Geometries
  # shellcheck disable=SC2154
  if [[ $loadHexGrid = true ]]; then
    LOG "Creating Elasticsearch indexes for Hexgrids"
    RUN python3 hexgrid_5km_postgres2es.py
    RUN python3 hexgrid_10km_postgres2es.py
    RUN python3 hexgrid_25km_postgres2es.py
    RUN python3 hexgrid_50km_postgres2es.py
    RUN python3 hexgrid_100km_postgres2es.py
    RUN python3 hexgrid_sauid_postgres2es.py

    LOG "Creating HexGrid Kibana Index Patterns"
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_5km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_5km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_10km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_10km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_25km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_25km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_50km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_50km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_100km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_100km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_sauid_hexgrid" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_sauid_hexgrid"}}'
  fi
}

load_kibana_saved_objects() {
  LOG "# Loading Kibana Saved Objects"
  RUN curl -X POST "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/_import" -H "kbn-xsrf: true" --form file=@kibanaSavedObjects.ndjson
}


############################################################################################
############    Define main function                                            ############
############################################################################################

main() {
  LOG "# Set up job"
  RUN setup_eatmydata
  RUN check_environment_variables
  RUN read_github_token
  RUN get_model_factory_scripts
  RUN get_git_lfs_pointers_of_csv_files
  RUN wait_for_postgres
  # Speed up PostgreSQL operations
  RUN set_synchronous_commit off


  LOG "# Process Exposure and Ancillary Data"
  RUN import_census_boundaries
  RUN import_physical_exposure_model
  RUN import_vs30_model
  RUN import_census_data
  RUN import_sovi
  RUN import_luts
  # RUN import_retrofit_costs
  RUN import_ghsl
  RUN import_mh_intensity
  RUN import_hazard_threat_thresholds
  RUN post_process_mh_tables
  RUN copy_ancillary_tables
  RUN post_process_all_tables_update
  RUN generate_indicators

  LOG "# Process PSRA"
  RUN import_raw_psra_tables
  RUN post_process_psra_tables

  LOG "# Process DSRA"
  RUN import_earthquake_scenarios
  RUN import_shakemap
  RUN import_rupture_model
  RUN create_scenario_risk_master_tables


  LOG "# Import data from PostGIS to Elasticsearch"
  RUN export_to_elasticsearch
  RUN load_kibana_saved_objects


  LOG "# Almost done!  Wrapping up..."

  # Restore PostgreSQL synchronous_commit default setting (on) for reliability
  RUN set_synchronous_commit on
  RUN sync

  echo
  LOG "=================================================================="
  LOG "Congratulations!"
  LOG "add_data.sh ran successfully to the end."
  LOG "Run 'docker compose logs -t python-opendrr' to check for warnings."
  LOG "Press Ctrl+C to exit."
  LOG "=================================================================="

  tail -f /dev/null & wait
}

main "$@"
