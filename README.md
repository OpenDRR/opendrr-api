# opendrr-api

REST API for OpenDRR data

[![GitHub Super-Linter](https://github.com/OpenDRR/opendrr-api/workflows/Lint%20Code%20Base/badge.svg)](https://github.com/marketplace/actions/super-linter)
![GitHub](https://img.shields.io/github/license/OpenDRR/opendrr-api)
<img src="https://github.com/OpenDRR/documentation/blob/master/models/opendrr-stack.png" width="600">

## Setup in your local environment

### 1. Prerequisites

- Docker engine installed and running: <https://docs.docker.com/get-docker/>
- Download or clone this repository to your local development environment

### 2. Edit the Docker environment settings

Make a copy of the `sample.env` file and rename it to `.env`. Make changes if required otherwise leave the default settings.

The settings below can be found in the .env file and control which data sets are loaded into ElasticSearch and Kibana:

loadDsraScenario=true
loadPsraModels=true
loadHazardThreat=false
loadPhysicalExposure=true
loadRiskDynamics=true
loadSocialFabric=true

### 3. Edit the Python container configuration

Make a copy of `python/sample_config.ini` and rename it `config.ini`. Open this file in an editor, add the required github_token (see <https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token>) and set the remaining parameters as follows:

    [auth]
    # Github Token for Private Repo Accesss
    github_token = a00049ba79152d03380c34652f2cb612

    [rds]
    # PostGIS Connection Details
    postgres_host = db-opendrr
    postgres_port = 5432
    postgres_un = postgres
    postgres_pw = password
    postgres_db = opendrr
    postgres_address = db-opendrr:5432/opendrr

    [es]
    # Elasticsearch Connection Details
    es_un = elastic
    es_pw = changeme
    es_endpoint = elasticsearch-opendrr:9200
    kibana_endpoint = localhost:5601

### 4. Run docker-compose

    docker-compose up --build

> NOTE: you will see errors thrown by the opendrr-api_pygeoapi-opendrr_1 container as the stack builds. These can be ignored.

Once the stack is built (~20min) you will need to verify that everything is working.

> NOTE: you can stop the stack whenever you like with `Ctrl-C` or `docker-compose stop`. See below on how you can bring the stack back up without re-building.

### 5. Verify that everything is working

Check Elasticsearch to ensure that the indexes were created:

<http://localhost:9200/_cat/indices?v&pretty>

You should see something similar to:

    health status index ...
    green  open   afm7p2_lrdmf_scenario_shakemap_intensity_building
    green  open   .apm-custom-link
    green  open   afm7p2_lrdmf_damage_state_building
    green  open   .kibana_task_manager_1
    green  open   afm7p2_lrdmf_social_disruption_building
    green  open   .apm-agent-configuration
    green  open   afm7p2_lrdmf_recovery_time_building
    green  open   afm7p2_lrdmf_scenario_shakemap_intensity_building
    green  open   afm7p2_lrdmf_casualties_building
    green  open   .kibana_1

You can explore the indexes in Elasticsearch using Kibana

<http://localhost:5601>

Check pygeoapi to make sure the collections can be accessed

<http://localhost:5001/collections>

Feature collections can be accessed as follows or by clicking on the links provided on the collections page

<http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building/items?f=json&limit=1>

You should see something similar to:

    {
        "type": "FeatureCollection",
        "features": [
            {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [
                -117.58484079,
                49.58943143
                ]
            },
            "properties": {
                "AssetID": "59002092-RES3A-W1-PC",
                "Sauid": "59002092",
                "sL_Asset_b0": 27.602575,
                "sL_Bldg_b0": 27.602575,
                "sLr_Bldg_b0": 1,
                "sLr2_BCR_b0": 0.000819,
                "SLr2_RoI": 0.001359,
                "sL_Str_b0": 27.602575,
                "sLsd_Str_b0": 23.4638,
                "sL_NStr_b0": 0,
                "sLsd_NStr_b0": 0,
                "sL_Cont_b0": 0,
                "sLsd_Cont_b0": 0,
                "sL_Asset_r2": 0.311704,
                "sL_Bldg_r2": 0.311704,
                "sLr_Bldg_r2": 1,
                "sL_Str_r2": 0.311704,
                "sLsd_Str_r2": 0.264966,
                "sL_NStr_r2": 0,
                "sLsd_NStr_r2": 0,
                "sL_Cont_r2": 0,
                "sLsd_Cont_r2": 0,
                "geom_point": "0101000020E6100000AD9A10086E655DC0D18A357D72CB4840"
            },
            "id": "59002092"
            }
        ],
        "numberMatched": 173630,
        "numberReturned": 1,
        "links": [
            {
            "type": "application/geo+json",
            "rel": "self",
            "title": "This document as GeoJSON",
            "href": "http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building/items?f=json&amp;limit=1"
            },
            {
            "rel": "alternate",
            "type": "application/ld+json",
            "title": "This document as RDF (JSON-LD)",
            "href": "http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building/items?f=jsonld&amp;limit=1"
            },
            {
            "type": "text/html",
            "rel": "alternate",
            "title": "This document as HTML",
            "href": "http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building/items?f=html&amp;limit=1"
            },
            {
            "type": "application/geo+json",
            "rel": "next",
            "title": "items (next)",
            "href": "http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building/items?startindex=1&amp;limit=1"
            },
            {
            "type": "application/json",
            "title": "Economic loss buildings",
            "rel": "collection",
            "href": "http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building"
            }
        ],
        "timeStamp": "2020-08-18T22:46:10.513010Z"
        }

## Interacting with the endpoints

### Querying pygeoapi

Refer to the pygeoapi documentation for general guidance:

<!-- textlint-disable -->
<http://localhost:5001/openapi?f=html>
<!-- textlint-enable -->`

> NOTE: querying is currently limited to spatial extent and exact value queries. For more complex querying use Elasticsearch (see below).

#### To filter on a specfic attribute

<http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building/items?sH_Mag=7.2>

#### To filter using a bounding box

<http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building/items?bbox=-119,48.8,-118.9,49.8&f=json>

### Querying Elasticsearch

#### Range query

<http://localhost:9200/afm7p2_lrdmf_scenario_shakemap_intensity_building/_search?q=properties.sH_PGA:[0.047580+TO+0.047584]>

OR using curl:

    curl -XGET "http://localhost:9200/afm7p2_lrdmf_scenario_shakemap_intensity_building/_search" -H 'Content-Type:
    application/json' -d'
    {
        "query": {
            "range": {
                "properties.sH_PGA": {
                    "gte": 0.047580,
                    "lte": 0.047584
                }
            }
        }
    }'

#### Specific value

<http://localhost:9200/afm7p2_lrdmf_scenario_shakemap_intensity_building/_search?q=properties.sH_PGA:0.047584>

OR using curl:

    curl -XGET "http://localhost:9200/afm7p2_lrdmf_scenario_shakemap_intensity_building/_search" -H 'Content-Type:
    application/json' -d'
    {
        "query": {
            "match": {
                "properties.sH_PGA" : 0.047584
            }
        }
    }'

#### Bounding box query

    curl -XGET "http://localhost:9200/afm7p2_lrdmf_scenario_shakemap_intensity_building/_search" -H 'Content-Type:
    application/json' -d'
    {
        "query": {
            "bool": {
                "filter": [
                    {
                        "geo_shape": {
                            "geometry": {
                                "shape": {
                                    "type": "envelope",
                                    "coordinates": [ [ -118.7, 50 ], [ -118.4, 49.9 ] ]
                                },
                                "relation": "intersects"
                            }
                        }
                    }
                ]
            }
        }
    }'

#### Nearest query

    curl -XGET "http://localhost:9200/nhsl_hazard_threat_all_indicators_s/_search" -H 'Content-Type:
    application/json' -d'
    {
      "query": {
        "geo_shape": {
          "geometry": {
            "shape": {
              "type": "circle",
              "radius": "20km",
              "coordinates": [ -118, 49 ]
            }
          }
        }
      }
    }'

## Interacting with the spatial database

The spatial database is implemented using PostGIS. You can connect to PostGIS using [pgAdmin](https://www.pgadmin.org/) (<https://www.pgadmin.org/>) with the connection parameters in your `.env` file. For example:

    POSTGRES_USER: postgres
    POSTGRES_PASSWORD: password
    POSTGRES_PORT: 5432
    DB_NAME: opendrr

### Adding datasets to QGIS

You have two options:

#### Connect to PostGIS

1. Add a "New Connection" by right clicking on the "PostGIS" data type in the browser
2. Enter a name for your connection (i.e. "OpenDRR")
3. Add the credentials as per your `.env` file (see above)
4. Click the "OK" button

#### Connect to OGC OpenAPI - Features

1. Add a "New Connection" by right clicking on the "WFS / OGC API -Features" data type in the browser
2. Enter a name for your connection (i.e. "OpenDRR")
3. Enter `http://localhost:5001` in the URL field
4. Select "OGC API - Features" in the "Version" dropdown
4. Click the "OK" button

## Start/Stop the stack

Once the stack is built you only need to re-build when there is new data. The `docker-compose-run.yml` script is an override that you can use to run the built stack - it doesn't create the python container that pulls the latest code and data from GitHub to populate the stack.

To start the stack:

    docker-compose -f docker-compose-run.yml start

To stop the stack:

    docker-compose -f docker-compose-run.yml stop

## Updating or rebuilding the stack

Take the stack down and remove the volumes:

    docker-compose down -v

Rebuild the stack:

    docker-compose up --build
