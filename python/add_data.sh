#!/bin/bash
trap : TERM INT
set -e

POSTGRES_USER=$1
POSTGRES_PASS=$2
POSTGRES_PORT=$3
DB_NAME=$4

DSRA_REPOSITORY=https://github.com/OpenDRR/scenario-catalogue/tree/master/FINISHED

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
  -L https://api.github.com/repos/OpenDRR/scenario-catalogue/contents/FINISHED
EQSCENARIO_LIST=`grep -P -o '"name": "s_lossesbyasset_*.*r2' FINISHED | cut -f3- -d_`
EQSCENARIO_LIST=($(echo $EQSCENARIO_LIST | tr ' ' '\n'))

EQSCENARIO_LIST_LONGFORM=`grep -P -o '"name": "s_lossesbyasset_*.*r2.*csv' FINISHED | cut -f3- -d_`
EQSCENARIO_LIST_LONGFORM=($(echo $EQSCENARIO_LIST_LONGFORM | tr ' ' '\n'))

for item in ${!EQSCENARIO_LIST[@]} 
do
    EQSCENARIO_LIST[item]=${EQSCENARIO_LIST[item]:0:${#EQSCENARIO_LIST[item]}-3}
done

# get model-factory scripts
git clone https://github.com/OpenDRR/model-factory.git --branch add-PSRA --depth 1 || (cd model-factory ; git pull)

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
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_DAUID.gpkg" -t_srs "EPSG:4326" -nln boundaries."Geometry_DAUID" -lco LAUNDER=NO
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_ERUID.gpkg" -t_srs "EPSG:4326" -nln boundaries."Geometry_ERUID" -lco LAUNDER=NO
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_FSAUID.gpkg" -t_srs "EPSG:4326" -nln boundaries."Geometry_FSAUID" -lco LAUNDER=NO
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_PRUID.gpkg" -t_srs "epsg:4326" -nln boundaries."Geometry_PRUID" -lco LAUNDER=NO
ogr2ogr -f "PostgreSQL" PG:"host=db-opendrr user=${POSTGRES_USER} dbname=${DB_NAME} password=${POSTGRES_PASS}" "boundaries/Geometry_SAUID.gpkg" -t_srs "EPSG:4326" -nln boundaries."Geometry_SAUID" -lco LAUNDER=NO
rm -rf boundaries
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Update_boundaries_SAUID_table.sql

echo "\n Importing scenario outputs into PostGIS..."
for eqscenario in ${EQSCENARIO_LIST[*]}
do
python3 DSRA_outputs2postgres_lfs.py --dsraModelDir=$DSRA_REPOSITORY --columnsINI=DSRA_outputs2postgres.ini --eqScenario=$eqscenario
done

echo "Importing Raw PSRA Tables"
#Get list of provinces & territories
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -O \
  -L https://api.github.com/repos/OpenDRR/canada-srm2/contents/cDamage/output
PT_LIST=`grep -P -o '"name": "*.*' output | cut -f2- -d:`
PT_LIST=($(echo $PT_LIST | tr ', ' '\n'))
for item in ${!PT_LIST[@]}
do
PT_LIST[item]=${PT_LIST[item]:1:${#PT_LIST[item]}-2}
done

#cDamage
for PT in ${PT_LIST[@]}
do
  curl -H "Authorization: token ${GITHUB_TOKEN}" \
    -O \
    -L https://api.github.com/repos/OpenDRR/canada-srm2/contents/cDamage/output/${PT}
  DOWNLOAD_LIST=`grep -P -o '"url": "*.*csv*.*' ${PT} | cut -f2- -d:`
  DOWNLOAD_LIST=($(echo $DOWNLOAD_LIST | tr ', ' '\n'))
  for item in ${!DOWNLOAD_LIST[@]}
  do
    DOWNLOAD_LIST[item]=${DOWNLOAD_LIST[item]:1:${#DOWNLOAD_LIST[item]}-2}
  done
  mkdir -p cDamage/${PT}/
  cd cDamage/${PT}/
  for file in ${DOWNLOAD_LIST[@]}
  do
    FILENAME=$(echo $file | cut -f-1 -d? | cut -f11- -d/)
    curl -H "Authorization: token ${GITHUB_TOKEN}" \
      -o $FILENAME \
      -L $file
    DOWNLOAD_URL=`grep -o '"download_url": *.*' ${FILENAME} | cut -f2- -d: | tr -d '"'| tr -d ',' `
    curl -o $FILENAME \
      -L $DOWNLOAD_URL
    sed -i '1d' $FILENAME
  done

  for file in cD_*dmg-mean_b0.csv
  do
    sed -i '1d' $file
    cat $file >> cD_${PT}_dmg-mean_b0_temp.csv
  done
  mv cD_${PT}_dmg-mean_b0_temp.csv cD_${PT}_dmg-mean_b0.csv

  for file in cD_*dmg-mean_r2.csv
  do
    sed -i '1d' $file
    cat $file > cD_${PT}_dmg-mean_r2_temp.csv
  done
  mv cD_${PT}_dmg-mean_r2_temp.csv cD_${PT}_dmg-mean_r2.csv
  
  cd /usr/src/app/
  rm -f ${PT}
done

#cHazard
for PT in ${PT_LIST[@]}
do
  curl -H "Authorization: token ${GITHUB_TOKEN}" \
    -O \
    -L https://api.github.com/repos/OpenDRR/canada-srm2/contents/cHazard/output/${PT}
  DOWNLOAD_LIST=`grep -P -o '"url": "*.*csv*.*' ${PT} | cut -f2- -d:`
  DOWNLOAD_LIST=($(echo $DOWNLOAD_LIST | tr ', ' '\n'))
  for item in ${!DOWNLOAD_LIST[@]}
  do
    DOWNLOAD_LIST[item]=${DOWNLOAD_LIST[item]:1:${#DOWNLOAD_LIST[item]}-2}
  done
  mkdir -p cHazard/${PT}/
  cd cHazard/${PT}/
  for file in ${DOWNLOAD_LIST[@]}
  do
    FILENAME=$(echo $file | cut -f-1 -d? | cut -f11- -d/)
    curl -H "Authorization: token ${GITHUB_TOKEN}" \
      -o $FILENAME \
      -L $file
    DOWNLOAD_URL=`grep -o '"download_url": *.*' ${FILENAME} | cut -f2- -d: | tr -d '"'| tr -d ',' `
    curl -o $FILENAME \
      -L $DOWNLOAD_URL
    sed -i '1d' $FILENAME
  done
  cd /usr/src/app/
  rm -f ${PT}
done

#eDamage
for PT in ${PT_LIST[@]}
do
  curl -H "Authorization: token ${GITHUB_TOKEN}" \
    -O \
    -L https://api.github.com/repos/OpenDRR/canada-srm2/contents/eDamage/output/${PT}
  DOWNLOAD_LIST=`grep -P -o '"url": "*.*csv*.*' ${PT} | cut -f2- -d:`
  DOWNLOAD_LIST=($(echo $DOWNLOAD_LIST | tr ', ' '\n'))
  for item in ${!DOWNLOAD_LIST[@]}
  do
    DOWNLOAD_LIST[item]=${DOWNLOAD_LIST[item]:1:${#DOWNLOAD_LIST[item]}-2}
  done
  mkdir -p eDamage/${PT}/
  cd eDamage/${PT}/
  for file in ${DOWNLOAD_LIST[@]}
  do
    FILENAME=$(echo $file | cut -f-1 -d? | cut -f11- -d/)
    curl -H "Authorization: token ${GITHUB_TOKEN}" \
      -o $FILENAME \
      -L $file
    DOWNLOAD_URL=`grep -o '"download_url": *.*' ${FILENAME} | cut -f2- -d: | tr -d '"'| tr -d ',' `
    curl -o $FILENAME \
      -L $DOWNLOAD_URL
    sed -i '1d' $FILENAME
  done

  for file in eD_*damages-mean_b0.csv
  do
    sed -i '1d' $file
    cat $file >> eD_${PT}_damages-mean_b0_temp.csv
  done
  mv eD_${PT}_damages-mean_b0_temp.csv eD_${PT}_damages-mean_b0.csv

  for file in eD_*damages-mean_r2.csv
  do
    sed -i '1d' $file
    cat $file > eD_${PT}_damages-mean_r2_temp.csv
  done
  mv eD_${PT}_damages-mean_r2_temp.csv eD_${PT}_damages-mean_r2.csv
  
  cd /usr/src/app/
  rm -f ${PT}
done

#ebRisk
for PT in ${PT_LIST[@]}
do
  curl -H "Authorization: token ${GITHUB_TOKEN}" \
    -O \
    -L https://api.github.com/repos/OpenDRR/canada-srm2/contents/ebRisk/output/${PT}
  DOWNLOAD_LIST=`grep -P -o '"url": "*.*csv*.*' ${PT} | cut -f2- -d:`
  DOWNLOAD_LIST=($(echo $DOWNLOAD_LIST | tr ', ' '\n'))
  for item in ${!DOWNLOAD_LIST[@]}
  do
    DOWNLOAD_LIST[item]=${DOWNLOAD_LIST[item]:1:${#DOWNLOAD_LIST[item]}-2}
  done
  mkdir -p ebRisk/${PT}/
  cd ebRisk/${PT}/
  for file in ${DOWNLOAD_LIST[@]}
  do
    FILENAME=$(echo $file | cut -f-1 -d? | cut -f11- -d/)
    curl -H "Authorization: token ${GITHUB_TOKEN}" \
      -o $FILENAME \
      -L $file
    DOWNLOAD_URL=`grep -o '"download_url": *.*' ${FILENAME} | cut -f2- -d: | tr -d '"'| tr -d ',' `
    curl -o $FILENAME \
      -L $DOWNLOAD_URL
    sed -i '1d' $FILENAME
  done

  # for PT in ${PT_LIST[@]}
  # do
  # echo $PT
  # python3 PSRA_combineSrcLossTable.py --srcLossDir=/usr/src/app/ebRisk/${PT}
  # done

  for file in ebR_*agg_curves-stats_b0.csv
  do
    sed -i '1d' $file
    cat $file >> ebR_${PT}_agg_curves-stats_b0_temp.csv
  done
  mv ebR_${PT}_agg_curves-stats_b0_temp.csv ebR_${PT}_agg_curves-stats_b0.csv

  for file in ebR_*agg_curves-stats_r2.csv
  do
    sed -i '1d' $file
    cat $file > ebR_${PT}_agg_curves-stats_r2_temp.csv
  done
  mv ebR_${PT}_agg_curves-stats_r2_temp.csv ebR_${PT}_agg_curves-stats_r2.csv

  for file in ebR_*agg_losses-stats_b0.csv
  do
    sed -i '1d' $file
    cat $file >> ebR_${PT}_agg_losses-stats_b0_temp.csv
  done
  mv ebR_${PT}_agg_losses-stats_b0_temp.csv ebR_${PT}_agg_losses-stats_b0.csv

  for file in ebR_*agg_losses-stats_r2.csv
  do
    sed -i '1d' $file
    cat $file > ebR_${PT}_agg_losses-stats_r2_temp.csv
  done
  mv ebR_${PT}_agg_losses-stats_r2_temp.csv ebR_${PT}_agg_losses-stats_r2_temp.csv

  for file in ebR_*avg_losses-stats_b0.csv
  do
    sed -i '1d' $file
    cat $file >> ebR_${PT}_avg_losses-stats_b0_temp.csv
  done
  mv ebR_${PT}_avg_losses-stats_b0_temp.csv ebR_${PT}_avg_losses-stats_b0.csv

  for file in ebR_*avg_losses-stats_r2.csv
  do
    sed -i '1d' $file
    cat $file > ebR_${PT}_avg_losses-stats_r2_temp.csv
  done
  mv ebR_${PT}_avg_losses-stats_r2_temp.csv ebR_${PT}_avg_losses-stats_r2.csv

  #Combine source loss tables for runs that were split by economic region or sub-region
  python3 PSRA_combineSrcLossTable.py --srcLossDir=/usr/src/app/ebRisk/${PT}
  
  cd /usr/src/app/
  rm -f ${PT}
done

#PSRA_0
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f psra_0.create_psra_schema.sql

#PSRA_1-8
for PT in ${PT_LIST[@]}
do 
python3 PSRA_sqlWrapper.py --province=${PT} --sqlScript="psra_1.Create_table_chazard_ALL.sql"
python3 PSRA_sqlWrapper.py --province=${PT} --sqlScript="psra_2.Create_table_dmg_mean.sql"
python3 PSRA_sqlWrapper.py --province=${PT} --sqlScript="psra_3.Create_table_agg_curves_stats.sql"
python3 PSRA_sqlWrapper.py --province=${PT} --sqlScript="psra_3.Create_table_avg_losses_stats.sql"
python3 PSRA_sqlWrapper.py --province=${PT} --sqlScript="psra_3.Create_table_src_loss_table.sql"
python3 PSRA_sqlWrapper.py --province=${PT} --sqlScript="psra_4.Create_psra_building_all_indicators.sql"
python3 PSRA_sqlWrapper.py --province=${PT} --sqlScript="psra_5.Create_psra_sauid_all_indicators.sql"
python3 PSRA_sqlWrapper.py --province=${PT} --sqlScript="psra_6.Create_psra_sauid_references_indicators.sql"
# python3 PSRA_sqlWrapper.py --province=${PT} --sqlScript="psra_7....."
# python3 PSRA_sqlWrapper.py --province=${PT} --sqlScript="psra_8....."
done


echo "\n Importing Physical Exposure Model into PostGIS"
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -o BldgExpRef_CA_master_v3p1.csv \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/exposure/general-building-stock/BldgExpRef_CA_master_v3p1.csv

DOWNLOAD_URL=`grep -o '"download_url": *.*' BldgExpRef_CA_master_v3p1.csv | cut -f2- -d: | tr -d '"'| tr -d ',' `
curl -o BldgExpRef_CA_master_v3p1.csv \
  -L $DOWNLOAD_URL
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_table_canada_exposure.sql

curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -o PhysExpRef_MetroVan_v4.csv \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/exposure/building-inventory/metro-vancouver/PhysExpRef_MetroVan_v4.csv

DOWNLOAD_URL=`grep -o '"download_url": *.*' PhysExpRef_MetroVan_v4.csv | cut -f2- -d: | tr -d '"'| tr -d ',' `
curl -o PhysExpRef_MetroVan_v4.csv \
  -L $DOWNLOAD_URL
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_table_canada_site_exposure_ste.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_site_exposure_to_building_and_sauid.sql


echo "\n Importing VS30 Model into PostGIS..."
curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -O \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/earthquake/sites/regions/vs30_CAN_site_model_xref.csv
DOWNLOAD_URL=`grep -o '"download_url": *.*' vs30_CAN_site_model_xref.csv | cut -f2- -d: | tr -d '"'| tr -d ',' `
curl -o vs30_CAN_site_model_xref.csv \
  -L $DOWNLOAD_URL

curl -H "Authorization: token ${GITHUB_TOKEN}" \
  -O \
  -L https://api.github.com/repos/OpenDRR/model-inputs/contents/earthquake/sites/regions/site-vgrid_CA.csv
DOWNLOAD_URL=`grep -o '"download_url": *.*' site-vgrid_CA.csv | cut -f2- -d: | tr -d '"'| tr -d ',' `
curl -o site-vgrid_CA.csv \
  -L $DOWNLOAD_URL

psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_table_vs_30_CAN_site_model.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_table_vs_30_CAN_site_model_xref.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_table_vs_30_BC_CAN_model_update_site_exposure.sql


echo "Importing Shakemap"
#Make a list of Shakemaps in the repo and download the raw csv files
DOWNLOAD_URL_LIST=`grep -P -o '"url": "*.*s_shakemap_*.*csv' FINISHED | cut -f2- -d: | tr -d '"'| tr -d ',' | cut -f1 -d?`
DOWNLOAD_URL_LIST=($(echo $DOWNLOAD_URL_LIST | tr ' ' '\n'))
for shakemap in ${DOWNLOAD_URL_LIST[*]}
do
    #Curl the shakemap
    shakemap_filename=$( echo $shakemap | cut -f9- -d/ | cut -f1 -d?)
    curl -H "Authorization: token ${GITHUB_TOKEN}" \
      -o $shakemap_filename \
      -L $shakemap
    DOWNLOAD_URL=`grep -o '"download_url": *.*' ${shakemap_filename} | cut -f2- -d: | tr -d '"'| tr -d ',' `
      echo $DOWNLOAD_URL
    curl -o $shakemap_filename \
      -L $DOWNLOAD_URL
    #Run Create_table_shakemap.sql
    python3 DSRA_runCreateTableShakemap.py --shakemapFile=$shakemap_filename
done

#RunCreate_table_shakemap_update.sql or Create_table_shakemap_update_ste.sql   
SHAKEMAP_LIST=`grep -P -o '"name": "s_shakemap_*.*csv' FINISHED | cut -f2- -d: | cut -f2- -d'"'`
SHAKEMAP_LIST=($(echo $SHAKEMAP_LIST | tr ' ' '\n'))
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
    #echo "Site Model"
    python3 DSRA_runCreateTableShakemapUpdate.py --eqScenario=$eqscenario --exposureAgg=$SITE
    elif [ "$SITE" = "b" ]
    then
    #echo "Building Model"
    python3 DSRA_runCreateTableShakemapUpdate.py --eqScenario=$eqscenario --exposureAgg=$SITE
    fi
    echo " " 
done 


echo "\n Importing Rupture Model"
python3 DSRA_ruptures2postgres.py --dsraRuptureDir="https://github.com/OpenDRR/scenario-catalogue/tree/master/deterministic/ruptures" 

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
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_physical_exposure_building_indicators_PhysicalExposure_ste.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_physical_exposure_sauid_indicators_view_PhysicalExposure_ste.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_physical_exposure_site_level_indicators_PhysicalExposure_ste.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_risk_dynamics_indicators.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_social_vulnerability_sauid_indicators_SocialFabric.sql
psql -h db-opendrr -U ${POSTGRES_USER} -d ${DB_NAME} -a -f Create_MH_risk_sauid_ALL.sql

echo "\n Generating indicator views..."
for item in ${EQSCENARIO_LIST_LONGFORM[*]}
do
    SITE=$(echo $item | cut -f5- -d_ | cut -c 1-1)
    eqscenario=$(echo $item | cut -f-2 -d_)
    #echo $eqscenario
    #echo $SITE
    if [ "$SITE" = "s" ]
    then
        #echo "Site Model"
        python3 DSRA_createRiskProfileIndicators.py --eqScenario=$eqscenario --aggregation=site_level --exposureModel=site
        python3 DSRA_createRiskProfileIndicators.py --eqScenario=$eqscenario --aggregation=building --exposureModel=site
        python3 DSRA_createRiskProfileIndicators.py --eqScenario=$eqscenario --aggregation=sauid  --exposureModel=site
    elif [ "$SITE" = "b" ]
    then
        #echo "Building Model"
        python3 DSRA_createRiskProfileIndicators.py --eqScenario=$eqscenario --aggregation=building --exposureModel=building
        python3 DSRA_createRiskProfileIndicators.py --eqScenario=$eqscenario --aggregation=sauid  --exposureModel=building
    fi
done 

# make sure Elasticsearch is ready prior to creating indexes
until $(curl -sSf -XGET --insecure 'http://elasticsearch-opendrr:9200/_cluster/health?wait_for_status=yellow' > /dev/null); do
    printf 'No status yellow from Elasticsearch, trying again in 10 seconds \n'
    sleep 10
done

#Load Probabilistic Model Indicators
if [ "$loadPsraModels" = true ]
then
    echo "Creating PSRA indices in ElasticSearch"
    for PT in ${PT_LIST[*]}
    do
      python3 psra_postgres2es.py --province=$PT --dbview="all_indicators" --idField="building"
      python3 psra_postgres2es.py --province=$PT --dbview="all_indicators" --idField="sauid"
    done

    echo "Creating PSRA Kibana Index Patterns"
    curl -X POST -H "Content-Type: application/json" "http://kibana-opendrr:5601/api/saved_objects/index-pattern/psra*all_indicators_s" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"psra*all_indicators_s"}}'
    curl -X POST -H "Content-Type: application/json" "http://kibana-opendrr:5601/api/saved_objects/index-pattern/psra*all_indicators_b" -H "kbn-xsrf: true" -d '{ "attributes": { "title":"psra*all_indicators_b"}}'
fi

#Load Deterministid Model Indicators
if [ "$loadDsraScenario" = true ]
then
    for eqscenario in ${EQSCENARIO_LIST[*]}
    do
        echo "\nCreating elasticsearch indexes for DSRA..."
        python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="all_indicators" --idField="building"
        python3 dsra_postgres2es.py --eqScenario=$eqscenario --dbview="all_indicators" --idField="sauid"
    done  
fi


#Load Hazard Threat Views
if [ "$loadHazardThreat" = true ]
then
    #Cyclone
    python3 hazardThreat_postgres2es.py  --type="cy_threat_cy_wind_hazard" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="cy_threat_to_assets" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="cy_threat_to_buildings" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="cy_threat_to_people" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    #Earthquake
    python3 hazardThreat_postgres2es.py  --type="eq_seismic_hazard_intensity" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="eq_threat_to_assets" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="eq_threat_to_buildings" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="eq_threat_to_people" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    #Flooding
    python3 hazardThreat_postgres2es.py  --type="fl_threat_fl_inundation_hazard" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="fl_threat_to_assets" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="fl_threat_to_buildings" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="fl_threat_to_people" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    #landslide
    python3 hazardThreat_postgres2es.py  --type="ls_threat_debris_flow_hazard" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="ls_threat_to_assets" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="ls_threat_to_buildings" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="ls_threat_to_people" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    #Multi Hazard
    python3 hazardThreat_postgres2es.py  --type="mh_mh_intensity" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="mh_threat_to_assets" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="mh_threat_to_buildings" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="mh_threat_to_people" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    #Tsunami
    python3 hazardThreat_postgres2es.py  --type="ts_threat_ts_inundation_hazard" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="ts_threat_to_assets" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="ts_threat_to_buildings" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="ts_threat_to_people" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    #Wildfire
    python3 hazardThreat_postgres2es.py  --type="wf_threat_wf_hazard" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="wf_threat_to_assets" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="wf_threat_to_buildings" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 hazardThreat_postgres2es.py  --type="wf_threat_to_people" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
fi


#load physical exposure inidcators
if [ "$loadPhysicalExposure" = true ]
then
    python3 exposure_postgres2es.py --type="assets" --aggregation="building" --geometry=geom_point --idField="BldgID"
    python3 exposure_postgres2es.py --type="assets" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 exposure_postgres2es.py --type="building_function" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 exposure_postgres2es.py --type="building_type" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 exposure_postgres2es.py --type="buildings" --aggregation="building" --geometry=geom_point --idField="BldgID"
    python3 exposure_postgres2es.py --type="people" --aggregation="building" --geometry=geom_point --idField="BldgID"
    python3 exposure_postgres2es.py --type="people" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 exposure_postgres2es.py --type="settled_area" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
fi 

#load Risk Dynamics Views
if [ "$loadRiskDynamics" = true ]
then
    python3 riskDynamics_postgres2es.py --type="hazard_susceptibility" --aggregation="sauid" --geometry=geom_point --idField="ghslID"
    python3 riskDynamics_postgres2es.py --type="land_use_change" --aggregation="sauid" --geometry=geom_point --idField="ghslID"
    python3 riskDynamics_postgres2es.py --type="population_growth" --aggregation="sauid" --geometry=geom_point --idField="ghslID"
fi

#load Social Fabric Views
if [ "$loadSocialFabric" = true ]
then
    python3 socialFabric_postgres2es.py --type="family_structure" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 socialFabric_postgres2es.py --type="financial_agency" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 socialFabric_postgres2es.py --type="housing_conditions" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
    python3 socialFabric_postgres2es.py --type="individual_autonomy" --aggregation="sauid" --geometry=geom_poly --idField="Sauid"
fi


echo "\n Loading Kibana Saved Objects"
curl -X POST http://kibana-opendrr:5601/api/saved_objects/_import -H "kbn-xsrf: true" --form file=@kibanaSavedObjects.ndjson

tail -f /dev/null & wait