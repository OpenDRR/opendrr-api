# opendrr-api
REST API for OpenDRR data

![image description](https://github.com/OpenDRR/documentation/blob/master/models/OpenDRR%20API.png)

## Setup in your local environment

### Prerequisites

- Docker engine installed and running

### Edit the configuration

Make a copy of `sample_config.ini` and rename it `config.ini`. Open this file in an editor, add the required github_token (see https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token) and set the remaining parameters as follows:

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

### Run docker-compose

    $ docker-compose up --build

Once the stack is built (~20min) you can stop it with `Ctrl-C`. See below on how you can bring the stack back up without re-building.
  
### Verify that everything is working

Check Elasticsearch to ensure that the index was created

    $ http://localhost:9200/_cat/indices?v&pretty

You should see something similar to:

    health status index ...
    green  open   economic_loss_agg_view XnIFL7LNTBWupGSXJOFjig ...

Check pygeoapi to make sure that the feature collection can be accessed

    $ http://localhost:5000/collections/economic_loss/items?f=json&limit=1

You should see something similar to:

    {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "Sauid": 59007611,
                    "Source_Type": "simpleFaultRupture",
                    "Rupture_Name": "SIM6p8_CR2022",
                    "Magnitude": "6.8",
                    "Retrofit": "b0",
                    "AssetCostT": "465568966",
                    "BldgCostT": "317463008",
                    "sL_LossRatio": "0.803799459550501",
                    "sL_AssetLoss": "1888058",
                    "sL_BldgLoss": "1517620",
                    "sL_StrLoss": "705600",
                    "sL_NStrLoss": "812020",
                    "sL_ContLoss": "370438",
                    "geom_point": "0101000020E6100000BA3AC63625CB5EC09F210316CCA24840"
                },
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [ ... ]
                },
                "id": 59007611
            }
        ],
        "numberMatched": 15198,
        "numberReturned": 1,
        "links": [
            {
                "type": "application/geo+json",
                "rel": "self",
                "title": "This document as GeoJSON",
                "href": "http://localhost:5000/collections/economic_loss/items?f=json&limit=1"
            },
            {
                "rel": "alternate",
                "type": "application/ld+json",
                "title": "This document as RDF (JSON-LD)",
                "href": "http://localhost:5000/collections/economic_loss/items?f=jsonld&limit=1"
            },
            {
                "type": "text/html",
                "rel": "alternate",
                "title": "This document as HTML",
                "href": "http://localhost:5000/collections/economic_loss/items?f=html&limit=1"
            },
            {
                "type": "application/geo+json",
                "rel": "next",
                "title": "items (next)",
                "href": "http://localhost:5000/collections/economic_loss/items?startindex=1&limit=1"
            },
            {
                "type": "application/json",
                "title": "Economic Loss",
                "rel": "collection",
                "href": "http://localhost:5000/collections/economic_loss"
            }
        ],
        "timeStamp": "2020-03-25T19:21:13.065240Z"
    }

## Interacting with the endpoints

### Querying pygeoapi

Refer to the pygeoapi documentation for general guidance:

    http://localhost:5000/openapi?f=html

> NOTE: querying is currently limited to spatial extent and exact value queries. For more complex querying use Elasticsearch (see below).

#### To filter on a specfic attribute

    http://localhost:5000/collections/economic_loss/items?Magnitude=6.8

#### To filter using a bounding box

    http://localhost:5000/collections/economic_loss/items?bbox=-119,48,-118,49&f=json

### Querying Elasticsearch

#### Range query

    curl -XGET "http://localhost:9200/dsra_sim6p8_cr2022_rlz_1_b0_economic_loss_agg_view/_search" -H 'Content-Type: 
    application/json' -d'
    {  
        "query": {    
            "range": {      
                "properties.sL_AssetLoss": {        
                    "gte": 1800000,        
                    "lte": 2000000      
                }    
            }  
        }
    }'

#### Specific value

    curl -XGET "http://localhost:9200/dsra_sim6p8_cr2022_rlz_1_b0_economic_loss_agg_view/_search" -H 'Content-Type: 
    application/json' -d'
    {  
        "query": {    
            "match": {      
                "properties.sL_AssetLoss": 1888058    
            }  
        }
    }'

#### Bounding box query

    curl -XGET "http://localhost:9200/dsra_sim6p8_cr2022_rlz_1_b0_economic_loss_agg_view/_search" -H 'Content-Type: 
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
                                "coordinates": [ [ -119, 49 ], [ -118, 48 ] ]
                            },
                            "relation": "intersects"
                        }
                    }
                }
            ]
        }
    }'

## Start/Stop the stack

Once the stack is built you only need to re-build when there is new data. The `docker-compose-run.yml` script is an override that you can use to run the built stack - it doesn't create the python container that pulls the latest code and data from GitHub to populate the stack. 

To start the stack:

    $ docker-compose -f docker-compose-run.yml start

To stop the stack:

    $ docker-compose -f docker-compose-run.yml stop
