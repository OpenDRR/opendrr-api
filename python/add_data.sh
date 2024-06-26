#!/bin/bash
# SPDX-License-Identifier: MIT
#
# add_data.sh - Populate PostGIS database for Elasticsearch
#
# Copyright (C) 2020-2024 Government of Canada
#
# Main Authors: Drew Rotheram-Clarke <drew.rotheram-clarke@canada.ca>
#               Joost van Ulden <joost.vanulden@canada.ca>
#               Anthony Fok <anthony.fok@nrcan-rncan.gc.ca>

trap : TERM INT
set -e

############################################################################################
############    Define global variables                                         ############
############################################################################################

# Needed variables (defined in .env for Docker Compose, or in a Amazon ECS task definition)
ENV_VAR_LIST=(
  POSTGRES_USER POSTGRES_PASS POSTGRES_PORT POSTGRES_HOST DB_NAME
  POPULATE_DB
  KIBANA_ENDPOINT ES_ENDPOINT ES_USER ES_PASS
  loadDsraScenario loadPsraModels loadHazardThreat loadPhysicalExposure
  loadRiskDynamics loadSocialFabric
  processDSRA processPSRA
)

: "${ADD_DATA_PRINT_FUNCNAME:=true}"
: "${ADD_DATA_PRINT_LINENO:=true}"
: "${ADD_DATA_REDUCE_DISK_USAGE:=true}"
: "${ADD_DATA_DRY_RUN:=false}"

: "${PSRA_REPO:=OpenDRR/seismic-risk-model}"
: "${PSRA_REPO_REF:=master}"

: "${DSRA_REPO:=OpenDRR/earthquake-scenarios}"
: "${DSRA_REPO_REF:=master}"

PT_LIST=(AB BC MB NB NL NS NT NU ON PE QC SK YT)
# PT_LIST=(AB MB NB NL NS NT NU ON PE QC SK YT)

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
  local i lineno funcname

  # Print blank line before a new section
  [[ $# == 1 ]] && [[ "$1" =~ ^#{1,2}[[:space:]] ]] && echo

  if [[ "${ADD_DATA_PRINT_LINENO,,}" =~ ^(true|1|y|yes|on)$ ]] || \
     [[ "${ADD_DATA_PRINT_FUNCNAME,,}" =~ ^(true|1|y|yes|on)$ ]]
  then
    i=0
    while [[ "${FUNCNAME[i]}" =~ ^(LOG|RUN|INFO|WARN)$ ]]; do
      (( i += 1 ))
    done
    lineno=:${BASH_LINENO[i-1]}
    funcname=:${FUNCNAME[i]}
  fi
  echo -n "[${0##*/}$lineno$funcname]"

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

# CLEAN_UP deletes downloaded data files after they have been imported
# if ADD_DATA_REDUCE_DISK_USAGE is true.
CLEAN_UP() {
  [[ "${ADD_DATA_REDUCE_DISK_USAGE,,}" =~ ^(true|1|y|yes|on)$ ]] || return 0

  # TODO: 1. Reject "*"; 2. Use safe-rm
  for i in "${@}"; do
    target="/usr/src/app/$i"
    if [ -e "$target" ]; then
      size=$(du -sh "$target" | cut -f1 || true)
      echo "Deleting $target (${size})"
      rm -rf "$target"
    else
      echo "$target does not exist, skipping delete"
    fi
  done
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

download_luts() {
  LOG "## Downloading LUTs"
  # sourceTypes.csv was moved to scripts/sourceTypes.csv on 2023-05-29
  # on master branch, making into v1.1.0 release on 2023-09-12.
  # See https://github.com/OpenDRR/seismic-risk-model/pull/92
  RUN fetch_csv seismic-risk-model \
    scripts/sourceTypes.csv${PSRA_REPO_REF:+?ref=$PSRA_REPO_REF}
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
    --retry-all-errors --retry-delay 5 --retry-max-time 0 --retry 360 \
    -H "Authorization: token ${GITHUB_TOKEN}" \
    -L "https://api.github.com/repos/$owner/$repo/contents/$path"

  is_dry_run || download_url=$(jq -r '.download_url' "$response")
  is_dry_run || size=$(jq -r '.size' "$response")

  # TODO: Actually use these values for verification
  echo download_url="$download_url"
  echo size="$size"

  LOG "Download from $download_url"
  RUN curl -o "$output_file" -L "$download_url" --retry-all-errors --retry-delay 5 --retry-max-time 0 --retry 360
}

# [OBSOLETE, to be refactored or be replaced with downloading release assets]
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
    --retry-all-errors --retry-delay 5 --retry-max-time 0 --retry 360 \
    -H "Authorization: token ${GITHUB_TOKEN}" \
    -H "Accept: application/vnd.github.v3+json" \
    -L "https://api.github.com/repos/$owner/$repo-xz/contents/$path_dir"

  is_dry_run || download_url=$(jq -r '.[] | select(.name == "'"$output_file"'.xz") | .download_url' "$response")
  LOG "${FUNCNAME[0]}: Download from $download_url"
  RUN curl -o "$output_file.xz" -L "$download_url" --retry-all-errors --retry-delay 5 --retry-max-time 0 --retry 360

  # TODO: Keep the compressed file somewhere, uncompress when needed
  RUN unxz "$output_file.xz"
}

# fetch_csv calls fetch_csv_lfs to fetch CSV files
fetch_csv() {
  # TODO: Make it more intelligent.
  RUN fetch_csv_lfs "$@"
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
    RUN readarray -t DOWNLOAD_LIST < <( \
      gh api "repos/${PSRA_REPO}/contents/${model}/output/${PT}${PSRA_REPO_REF:+?ref=$PSRA_REPO_REF}" \
        -q '.[].url | select(. | contains(".csv"))' \
    )

    mkdir -p "$model/$PT"
    ( cd "$model/$PT"
      for file in "${DOWNLOAD_LIST[@]}"; do
        FILENAME=$(echo "$file" | cut -f-1 -d? | cut -f11- -d/)
        RUN curl -H "Authorization: token ${GITHUB_TOKEN}" \
          --retry-all-errors --retry-delay 5 --retry-max-time 0 --retry 360 \
          -o "$FILENAME" \
          -L "$file"
        is_dry_run || DOWNLOAD_URL=$(jq -r '.download_url' "$FILENAME")
        RUN curl -o "$FILENAME" \
          --retry-all-errors --retry-delay 5 --retry-max-time 0 --retry 360 \
          -L "$DOWNLOAD_URL"

        # Strip OpenQuake comment header if exists
        # (safe for cH_${PT}_hmaps_xref.csv)
        RUN sed -i -r $'1{/^(\xEF\xBB\xBF)?#,/d}' "$FILENAME"
      done
    )
  done
}

fetch_psra_csv_from_national_model() {
  if [ "$#" != "1" ]; then
    ERROR "${FUNCNAME[0]} requires exactly one argument, but $# was given."
  fi

  model=$1
  PT=Canada

  RUN readarray -t DOWNLOAD_LIST < <( \
    gh api "repos/${PSRA_REPO}/contents/${model}/output/Canada${PSRA_REPO_REF:+?ref=$PSRA_REPO_REF}" \
      -q '.[].url | select(. | contains(".csv"))' \
  )

  mkdir -p "$model/$PT"
  ( cd "$model/$PT"
    for file in "${DOWNLOAD_LIST[@]}"; do
      FILENAME=$(echo "$file" | cut -f-1 -d? | cut -f11- -d/)
      RUN curl -H "Authorization: token ${GITHUB_TOKEN}" \
        --retry-all-errors --retry-delay 5 --retry-max-time 0 --retry 360 \
        -o "$FILENAME" \
        -L "$file"
      is_dry_run || DOWNLOAD_URL=$(jq -r '.download_url' "$FILENAME")
      RUN curl -o "$FILENAME" \
        --retry-all-errors --retry-delay 5 --retry-max-time 0 --retry 360 \
        -L "$DOWNLOAD_URL"

      # Strip OpenQuake comment header if exists
      # (safe for cH_${PT}_hmaps_xref.csv)
      RUN sed -i -r $'1{/^(\xEF\xBB\xBF)?#,/d}' "$FILENAME"
    done
  )

}

# merge_csv merges CSV files without repeating column headers.
# Syntax: merge_csv [INPUT_CSV_FILES]... [OUTPUT_FILE]
# NOTE: The '#,,,,,"generated_by='OpenQuake engine 3.x..."' comment header
#       is currently NOT removed removed by merge_csv()
#       but by fetch_psra_csv_from_model().  This may change in the future.
merge_csv() {
  if [ "$#" -lt "2" ]; then
    ERROR "${FUNCNAME[0]} requires at least two arguments, but $# was given."
  fi
  local input_files=("${@:1:$#-1}")
  local output_file="${*:$#}"

  INFO "merge_csv input: ${input_files[*]}"
  INFO "merge_csv output: $output_file"

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
  export GITHUB_TOKEN
  INFO "GITHUB_TOKEN is ${#GITHUB_TOKEN} characters in length"

  if [[ ${#GITHUB_TOKEN} -lt 40 ]]; then
    WARN "Your GITHUB_TOKEN has a length of ${#GITHUB_TOKEN} characters, but 40 or above is expected."
  fi

  # TEST_PRIVATE_REPO is set to any private Git repository within the
  # OpenDRR organization, for testing the validity of the user's GitHub token
  TEST_PRIVATE_REPO=OpenDRR/DSRA-processing
  TEST_PRIVATE_FILE=README.md

  tmpfile=$(mktemp)
  status_code=$(curl --write-out "%{http_code}" --silent --output "$tmpfile" \
    -H "Authorization: token ${GITHUB_TOKEN}" \
    -L https://api.github.com/repos/${TEST_PRIVATE_REPO}/contents/${TEST_PRIVATE_FILE})
  INFO "Access to test private repo ${TEST_PRIVATE_REPO} returns HTTP status code $status_code"

  if [[ "$status_code" -ne 200 ]] ; then
    cat "$tmpfile"
    case "$status_code" in
      401)
        ERROR "Your GitHub token is invalid or has expired. Aborting..."
        ;;
      404)
        ERROR "Your GitHub token was unable to access ${TEST_PRIVATE_REPO}. Please ensure the \"repo\" scope is enabled for the token. Aborting..."
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
  curl -L -o model-factory.tar.gz https://github.com/OpenDRR/model-factory/archive/refs/tags/v1.4.4.tar.gz
  tar -xf model-factory.tar.gz
  # RUN git clone https://github.com/OpenDRR/model-factory.git --branch updates_july2022 --depth 1 || (cd model-factory ; RUN git pull)

  # Copy model-factory scripts to working directory
  # TODO: Find ways to keep these scripts in their place without copying them all to WORKDIR
  RUN cp model-factory-1.4.4/scripts/*.* .
  # RUN cp model-factory/scripts/*.* .
  #rm -rf model-factory
}

# get_git_lfs_pointers_of_csv_files fetches from source data repositories
# Git LFS pointers of CSV files for "oid sha256" values that are used for
# checksum verification later in the program.
get_git_lfs_pointers_of_csv_files() {
  LOG '## Fetch Git LFS pointers of CSV files for "oid sha256"'
  local base_dir=git-sha256
  RUN rm -rf "$base_dir"
  RUN mkdir -p "$base_dir"
  ( cd "$base_dir" && \
    for repo in "${DSRA_REPO}" OpenDRR/openquake-inputs "${PSRA_REPO}"; do
      RUN git clone --filter=blob:none --no-checkout "https://${GITHUB_TOKEN}@github.com/${repo}.git"
      is_dry_run || \
        ( RUN cd "$(basename "$repo")" && \
          RUN git sparse-checkout set '*.csv' && \
          GIT_LFS_SKIP_SMUDGE=1 RUN git checkout )
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

# import_exposure_ancillary_db pulls postgres dump of exposure model
# and base layers and restores to empty database
import_exposure_ancillary_db() {
  LOG "Download exposure and base layer database dump and restore \
  to postgres"
  INFO "Download opendrr-exposure-ancillary.dump  (PostGIS database archive)"

  local repo="OpenDRR/opendrr-api"

  local base_branch="v1.4.4"

  if release_view=$(gh release view "${base_branch}" -R "${repo}"); then
    # For released version, we download from release assets
    INFO "... from release assets of ${repo} ${base_branch}..."

    for i in $(echo "${release_view}" | grep "^asset:	opendrr-exposure-ancillary\.7z" | cut -f2); do
      INFO "Downloading ${i}..."
      RUN gh release download "${base_branch}" -R "${repo}" --pattern "${i}"
    done

    if [[ -f opendrr-exposure-ancillary.7z.001 ]]; then
      7zz e opendrr-exposure-ancillary.7z.001
      # cat opendrr-exposure-ancillary.7z.[0-9][0-9] > opendrr-exposure-ancillary.dump
    fi
  fi

  # RUN ls -l opendrr-exposure-ancillary.dump*
  # RUN sha256sum -c opendrr-exposure-ancillary.dump.sha256sum
  CLEAN_UP opendrr-exposure-ancillary.7z.[0-9][0-9][0-9]

  INFO "Import opendrr-exposure-ancillary.dump using pg_restore..."

  # Note: Do not use "--create" which would activate the SQL command
  #   CREATE DATABASE boundaries WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE = 'English_United States.1252';
  # inside opendrr-boundaries.sql (generated on Windows), leading to
  #   error: invalid locale name: "English_United States.1252"
  # in the Linux-based Docker image
  RUN pg_restore -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$DB_NAME" \
    -j 8 --clean --if-exists --verbose opendrr-exposure-ancillary.dump

  CLEAN_UP opendrr-exposure-ancillary.dump
}

############################################################################################
############    Define "Process PSRA" functions                                 ############
############################################################################################

import_raw_psra_tables() {
  LOG "## Importing Raw PSRA Tables"

  LOG "### Get list of provinces & territories from ${PSRA_REPO}"
  RUN readarray -t FETCHED_PT_LIST < <( \
    gh api "repos/${PSRA_REPO}/contents/eDamage/output${PSRA_REPO_REF:+?ref=$PSRA_REPO_REF}" \
      -q '.[].name' \
  )
  if [[ "${PT_LIST[*]}" == "${FETCHED_PT_LIST[*]}" ]]; then
    LOG "PT_LIST and FETCHED_PT_LIST are equal: (${PT_LIST[*]})"
  else
    WARN "PT_LIST and FETCHED_PT_LIST differ:"
    WARN "Want: (${PT_LIST[*]})"
    WARN "Got : (${FETCHED_PT_LIST[*]})"
  fi

  # Disable cDamage.  As @wkhchow noted in commit 922c409:
  #   change cDamage reference to eDamage (cDamage will be removed eventually)
  # See also https://github.com/OpenDRR/opendrr-api/pull/201 (May 2022)
  #
  # LOG "### cDamage_beta"
  # INFO "cDamage was renamed to cDamage_beta as it is not part of the official release"
  # INFO "See https://github.com/OpenDRR/seismic-risk-model/pull/92"
  # RUN fetch_psra_csv_from_model cDamage_beta
  # LOG "Rename cDamage_beta back to cDamage"
  # RUN rm -rf cDamage
  # RUN mv -v cDamage_beta cDamage
  #
  # for PT in "${PT_LIST[@]}"; do
  #   ( cd "cDamage/$PT"
  #     RUN merge_csv cD_*dmg-mean_b0.csv "cD_${PT}_dmg-mean_b0.csv"
  #     RUN merge_csv cD_*dmg-mean_r2.csv "cD_${PT}_dmg-mean_r2.csv"
  #   )
  # done

  LOG "### cHazard_beta"
  INFO "cHazard was renamed to cHazard_beta as it is not part of the official release"
  INFO "See https://github.com/OpenDRR/seismic-risk-model/pull/92"
  RUN fetch_psra_csv_from_model cHazard_beta
  LOG "Rename cHazard_beta back to cHazard"
  RUN rm -rf cHazard
  RUN mv -v cHazard_beta cHazard

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
      RUN merge_csv eD_*damages-q05_b0.csv "eD_${PT}_damages-q05_b0.csv"
      RUN merge_csv eD_*damages-q05_r1.csv "eD_${PT}_damages-q05_r1.csv"
      RUN merge_csv eD_*damages-q95_b0.csv "eD_${PT}_damages-q95_b0.csv"
      RUN merge_csv eD_*damages-q95_r1.csv "eD_${PT}_damages-q95_r1.csv"
      RUN merge_csv eD_*damages-mean_b0.csv "eD_${PT}_damages-mean_b0.csv"
      RUN merge_csv eD_*damages-mean_r1.csv "eD_${PT}_damages-mean_r1.csv"
    )
  done

  LOG "### ebRisk"
  RUN fetch_psra_csv_from_model ebRisk

  for PT in "${PT_LIST[@]}"; do
    ( cd "ebRisk/$PT"
      # Add region fields to the agg losses and agg curves
      RUN python3 /usr/src/app/PSRA_combineAggCurvesStats.py --aggCurvesDir="/usr/src/app/ebRisk/$PT"
      RUN python3 /usr/src/app/PSRA_combineAggLossesStats.py --aggLossDir="/usr/src/app/ebRisk/$PT"

      RUN merge_csv ebR_*agg_curves-stats_b0.csv "ebR_${PT}_agg_curves-stats_b0.csv"
      RUN merge_csv ebR_*agg_curves-stats_r1.csv "ebR_${PT}_agg_curves-stats_r1.csv"
      RUN merge_csv ebR_*agg_curves-q05_b0.csv "ebR_${PT}_agg_curves-q05_b0.csv"
      RUN merge_csv ebR_*agg_curves-q05_r1.csv "ebR_${PT}_agg_curves-q05_r1.csv"
      RUN merge_csv ebR_*agg_curves-q95_b0.csv "ebR_${PT}_agg_curves-q95_b0.csv"
      RUN merge_csv ebR_*agg_curves-q95_r1.csv "ebR_${PT}_agg_curves-q95_r1.csv"
      RUN merge_csv ebR_*agg_losses-stats_b0.csv "ebR_${PT}_agg_losses-stats_b0.csv"
      RUN merge_csv ebR_*agg_losses-stats_r1.csv "ebR_${PT}_agg_losses-stats_r1.csv"
      RUN merge_csv ebR_*agg_losses-q05_b0.csv "ebR_${PT}_agg_losses-q05_b0.csv"
      RUN merge_csv ebR_*agg_losses-q05_r1.csv "ebR_${PT}_agg_losses-q05_r1.csv"
      RUN merge_csv ebR_*agg_losses-q95_b0.csv "ebR_${PT}_agg_losses-q95_b0.csv"
      RUN merge_csv ebR_*agg_losses-q95_r1.csv "ebR_${PT}_agg_losses-q95_r1.csv"
      RUN merge_csv ebR_*avg_losses-stats_b0.csv "ebR_${PT}_avg_losses-stats_b0.csv"
      RUN merge_csv ebR_*avg_losses-stats_r1.csv "ebR_${PT}_avg_losses-stats_r1.csv"

      # Combine source loss tables for runs that were split by economic region or sub-region
      RUN python3 /usr/src/app/PSRA_combineSrcLossTable.py --srcLossDir="/usr/src/app/ebRisk/$PT"
    )
  done

  LOG "### ebRisk - Canada"
  RUN fetch_psra_csv_from_national_model ebRisk
}

post_process_psra_tables() {
  LOG "## PSRA_0"
  RUN run_psql psra_0.create_psra_schema.sql

  LOG "## PSRA_1-6"
  for PT in "${PT_LIST[@]}"; do
    RUN python3 PSRA_runCreate_tables.py --province="$PT" --sqlScript="psra_1.Create_tables.sql"
    RUN python3 PSRA_copyTables.py --province="$PT"
    RUN python3 PSRA_sqlWrapper.py --province="$PT" --sqlScript="psra_2.Create_table_updates.sql"
    RUN python3 PSRA_sqlWrapper.py --province="$PT" --sqlScript="psra_3.Create_psra_building_all_indicators.sql"
    RUN python3 PSRA_sqlWrapper.py --province="$PT" --sqlScript="psra_4.Create_psra_sauid_all_indicators.sql"
    RUN python3 PSRA_sqlWrapper.py --province="$PT" --sqlScript="psra_5.Create_psra_sauid_references_indicators.sql"
  done

  LOG '## PSRA Canada'
  RUN run_psql psra_1.Create_tables_Canada.sql
  RUN python3 PSRA_copyTables_Canada.py
  RUN run_psql psra_2.Create_table_updates_Canada.sql
  RUN run_psql psra_4.Create_psra_sauid_all_indicators_Canada.sql

  RUN run_psql psra_6.Create_psra_merge_into_national_indicators.sql
  RUN run_psql psra_6a.eqri_calculation_sa.sql
  RUN run_psql psra_6a1.eqri_calculation_csd.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_1km_uc.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_1km_uc_3857.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_1km.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_1km_3857.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_5km_uc.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_5km_uc_3857.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_5km.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_5km_3857.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_10km_uc.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_10km_uc_3857.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_10km.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_10km_3857.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_25km_uc.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_25km_uc_3857.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_25km.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_25km_3857.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_50km_uc.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_50km_uc_3857.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_100km_uc.sql
  RUN run_psql psra_6a2.eqri_calculation_hexgrid_100km_uc_3857.sql
  RUN run_psql psra_6a3.Merge_eqri_calculations.sql

  RUN run_psql psra_7.Create_psra_national_hexgrid_clipped_unclipped.sql
  RUN run_psql psra_7.Create_psra_national_hexgrid_clipped_unclipped_3857.sql
  RUN run_psql psra_7.Create_psra_national_hexgrid_clipped.sql
  RUN run_psql psra_7.Create_psra_national_hexgrid_clipped_3857.sql
}

############################################################################################
############    Define "Process DSRA" functions                                 ############
############################################################################################

import_earthquake_scenarios() {
  LOG "## Get list of earthquake scenarios"
  gh api "repos/${DSRA_REPO}/contents/FINISHED${DSRA_REPO_REF:+?ref=$DSRA_REPO_REF}" > FINISHED.json

  # s_lossesbyasset_ACM6p5_Beaufort_r1_299_b.csv → ACM6p5_Beaufort
  RUN readarray -t EQSCENARIO_LIST < <(jq -r '.[].name | scan("(?<=s_lossesbyasset_).*(?=_r1)")' FINISHED.json)

  # s_lossesbyasset_ACM6p5_Beaufort_r1_299_b.csv → ACM6p5_Beaufort_r1_299_b.csv
  RUN readarray -t EQSCENARIO_LIST_LONGFORM < <(jq -r '.[].name | scan("(?<=s_lossesbyasset_).*r1.*\\.csv")' FINISHED.json)

  LOG "## Importing scenario outputs into PostGIS"
  for eqscenario in "${EQSCENARIO_LIST[@]}"; do
    RUN python3 DSRA_outputs2postgres_lfs.py \
                  --dsraRepo="${DSRA_REPO}" \
                  --dsraRepoBranch="${DSRA_REPO_REF}" \
                  --columnsINI=DSRA_outputs2postgres.ini \
                  --eqScenario="$eqscenario"
  done
}

import_shakemap() {
  LOG "## Importing Shakemap"
  # Make a list of Shakemaps in the repo and download the raw csv files
  readarray -t DOWNLOAD_URL_LIST < <(jq -r '.[].url | scan(".*s_shakemap_.*(?<!MMI)\\.csv")' FINISHED.json)
  for shakemap in "${DOWNLOAD_URL_LIST[@]}"; do
    # Get the shakemap
    shakemap_filename=$( echo "$shakemap" | cut -f9- -d/ | cut -f1 -d?)
    RUN curl -H "Authorization: token ${GITHUB_TOKEN}" \
      --retry-all-errors --retry-delay 5 --retry-max-time 0 --retry 360 \
      -o "$shakemap_filename" \
      -L "${shakemap}${DSRA_REPO_REF:+?ref=$DSRA_REPO_REF}"
    is_dry_run || DOWNLOAD_URL=$(jq -r '.download_url' "$shakemap_filename")
    LOG "$DOWNLOAD_URL"
    RUN curl -o "$shakemap_filename" \
      --retry-all-errors --retry-delay 5 --retry-max-time 0 --retry 360 \
      -L "$DOWNLOAD_URL"

    # Run Create_table_shakemap.sql
    RUN python3 DSRA_runCreateTableShakemap.py --shakemapFile="$shakemap_filename"
  done

  # Run Create_table_shakemap_update.sql or Create_table_shakemap_update_ste.sql
  RUN readarray -t SHAKEMAP_LIST < <(jq -r '.[].name | scan("s_shakemap_.*\\.csv")' FINISHED.json)
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
RUN python3 DSRA_ruptures2postgres.py \
      --dsraRuptureRepo="${DSRA_REPO}" \
      --dsraRuptureBranch="${DSRA_REPO_REF}"

LOG "## Generating indicator views"
  for item in "${EQSCENARIO_LIST_LONGFORM[@]}"; do
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

create_database_check() {
  LOG "## Create table to check row counts for each table/view"
  RUN run_psql Database_check.sql
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
  if [[ $loadPsraModels == true ]]; then
    LOG "Creating PSRA indices in Elasticsearch"
    RUN python3 psra_postgres2es.py
    RUN python3 srcLoss_postgres2es.py
    RUN python3 fsa_postgres2es.py

    LOG "Creating PSRA Kibana Index Patterns"
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_psra_indicators_s" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_psra_indicators_s"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_psra_indicators_b" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_psra_indicators_b"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_psra_hmaps" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_psra_hmaps"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_psra_uhs" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_psra_uhs"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_psra_srcLoss" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_psra_srcLoss"}}'
  fi

  # Load Deterministic Model Indicators
  # shellcheck disable=SC2154
  if [[ $loadDsraScenario == true ]]; then
    for eqscenario in "${EQSCENARIO_LIST[@]}"; do
      LOG "Creating Elasticsearch indexes for DSRA"
      #RUN python3 dsra_postgres2es.py --eqScenario="$eqscenario" --dbview="indicators" --idField="building"
      RUN python3 dsra_postgres2es.py --eqScenario="$eqscenario"
      RUN python3 dsraShakemap_postgres2es.py --eqScenario="$eqscenario"

      # LOG "Creating DSRA Kibana Index Patterns"
      # Need to develop saved object workflow for automated index patern generation
      # RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_dsra_${eqscenario}_indicators_s" -H "kbn-xsrf: true" -d "{ 'attributes': { 'title':'opendrr_dsra_${eqscenario}_indicators_s'}}"
      # RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_dsra_${eqscenario}_indicators_b" -H "kbn-xsrf: true" -d "{ 'attributes': { 'title':'opendrr_dsra_${eqscenario}_indicators_b'}}"
    done
  RUN python3 dsraExtents_postgres2es.py
  fi

  # Load Hazard Threat Views
  # 2021/09/21 DR - Keeping Hazard Threah and Risk Dynamics out of ES for the time being
  # shellcheck disable=SC2154
  # if [[ $loadHazardThreat == true ]]; then
  #   # All Indicators
  #   LOG "Creating Elasticsearch indexes for Hazard Threat"
  #   RUN python3 hazardThreat_postgres2es.py  --type="indicators" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"

  #   LOG "Creating Hazard Threat Kibana Index Patterns"
  #   RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hazard_threat_indicators_s" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hazard_threat_indicators_s"}}'
  # fi

  # Load physical exposure indicators
  # shellcheck disable=SC2154
  if [[ $loadPhysicalExposure == true ]]; then
    LOG "Creating Elasticsearch indexes for Physical Exposure"
    RUN python3 exposure_postgres2es.py

    LOG "Creating Exposure Kibana Index Patterns"
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_physical_exposure_indicators_s" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_physical_exposure_indicators_s"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_physical_exposure_indicators_b" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_physical_exposure_indicators_b"}}'
  fi

  # Load Risk Dynamics Views
  # 2021/09/21 DR - Keeping Hazard Threah and Risk Dynamics out of ES for the time being
  # shellcheck disable=SC2154
  # if [[ $loadRiskDynamics == true ]]; then
  #   LOG "Creating Elasticsearch indexes for Risk Dynamics"
  #   RUN python3 riskDynamics_postgres2es.py --type="indicators" --aggregation="sauid" --geometry=geom_point --idField="ghslID"

  #   LOG "Creating Risk Dynamics Kibana Index Patterns"
  #   RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_risk_dynamics_indicators" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_risk_dynamics_indicators"}}'
  # fi

  # Load Social Fabric Views
  # shellcheck disable=SC2154
  if [[ $loadSocialFabric == true ]]; then
    LOG "Creating Elasticsearch indexes for Social Fabric"
    RUN python3 socialFabric_postgres2es.py --aggregation="sauid" --geometry=geom_poly --sortfield="Sauid"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_1km" --geometry=geom --sortfield="gridid_1"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_1km_uc" --geometry=geom --sortfield="gridid_1"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_5km" --geometry=geom --sortfield="gridid_5"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_5km_uc" --geometry=geom --sortfield="gridid_5"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_10km" --geometry=geom --sortfield="gridid_10"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_10km_uc" --geometry=geom --sortfield="gridid_10"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_25km" --geometry=geom --sortfield="gridid_25"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_25km_uc" --geometry=geom --sortfield="gridid_25"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_50km_uc" --geometry=geom --sortfield="gridid_50"
    RUN python3 socialFabric_postgres2es.py --aggregation="hexgrid_100km_uc" --geometry=geom --sortfield="gridid_100"

    LOG "Creating Social Fabric Kibana Index Patterns"
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_social_fabric_indicators_s" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_social_fabric_indicators_s"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_social_fabric_indicators_hexgrid_5km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_social_fabric_indicators_hexgrid_5km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_social_fabric_indicators_hexgrid_10km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_social_fabric_indicators_hexgrid_10km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_social_fabric_indicators_hexgrid_25km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_social_fabric_indicators_hexgrid_25km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_social_fabric_indicators_hexgrid_50km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_social_fabric_indicators_hexgrid_50km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_nhsl_social_fabric_indicators_hexgrid_100km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_nhsl_social_fabric_indicators_hexgrid_100km"}}'
  fi

  # Load Hexgrid Geometries
  # shellcheck disable=SC2154
  if [[ $loadHexGrid == true ]]; then
    LOG "Creating Elasticsearch indexes for Hexgrids"
    RUN python3 hexgrid_1km_postgres2es.py
    RUN python3 hexgrid_1km_unclipped_postgres2es.py
    RUN python3 hexgrid_5km_postgres2es.py
    RUN python3 hexgrid_5km_unclipped_postgres2es.py
    RUN python3 hexgrid_10km_postgres2es.py
    RUN python3 hexgrid_10km_unclipped_postgres2es.py
    RUN python3 hexgrid_25km_postgres2es.py
    RUN python3 hexgrid_25km_unclipped_postgres2es.py
    RUN python3 hexgrid_50km_unclipped_postgres2es.py
    RUN python3 hexgrid_100km_unclipped_postgres2es.py
    RUN python3 hexgrid_sauid_postgres2es.py
    RUN python3 hexgrid_sauid_unclipped_postgres2es.py

    LOG "Creating HexGrid Kibana Index Patterns"
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_1km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_1km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_1km_unclipped" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_1km_unclipped"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_5km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_5km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_5km_unclipped" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_5km_unclipped"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_10km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_10km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_10km_unclipped" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_10km_unclipped"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_25km" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_25km"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_25km_unclipped" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_25km_unclipped"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_50km_unclipped" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_50km_unclipped"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_hexgrid_100km_unclipped" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_hexgrid_100km_unclipped"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_sauid_hexgrid" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_sauid_hexgrid"}}'
    RUN curl -X POST -H "Content-Type: application/json" "${KIBANA_ENDPOINT}/s/gsc-cgc/api/saved_objects/index-pattern/opendrr_sauid_hexgrid_unclipped" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"opendrr_sauid_hexgrid_unclipped"}}'
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
  # TODO: Use get_git_lfs_pointers_of_csv_files when CSV compression is implemented.
  # RUN get_git_lfs_pointers_of_csv_files
  RUN wait_for_postgres
  # Speed up PostgreSQL operations
  RUN set_synchronous_commit off


  LOG "# Restore exposure and ancillary database"
  RUN import_exposure_ancillary_db
  RUN download_luts

  # shellcheck disable=SC2154
  if [[ $processPSRA == true ]]; then
    LOG "# Processing PSRA"
    RUN import_raw_psra_tables
    RUN post_process_psra_tables
  else
    LOG "# Omitting PSRA Processing"
  fi

  # shellcheck disable=SC2154
  if [[ $processDSRA == true ]]; then
    LOG "# Process DSRA"
    RUN import_earthquake_scenarios
    RUN import_shakemap
    RUN import_rupture_model
    RUN create_scenario_risk_master_tables
    RUN create_database_check
  else
    LOG "# Omitting DSRA Processing"
  fi

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
