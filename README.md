# opendrr-api
REST API for OpenDRR data

## Prerequisites
 - Elasticsearch 7.1.0+ running locally on port 9200
    - E.g. http://localhost:9200/
- PygeoAPI 0.7.0+ with Elasticsearch provider running locally
    - E.g. http://localhost:5000/
- GeoJSON file(s)
    - Sample provided in `sample-data` directory

## Setup

Install and start Elasticsearch on localhost

    $ docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.6.2

> NOTE: if you have Elasticsearch installed on localhost already simply start it:

    $ elasticsearch

Add `dataset` to PygeoAPI using the Elasticsearch provider

    datasets:
        economic_loss:
            title: Economic Loss
            description: Economic consequences aggregated
            keywords:
                - earthquake
            links:
                - type: text/html
                rel: canonical
                title: information
                href: http://www.riskprofiler.ca/
                hreflang: en-US
            extents:
                spatial:
                    bbox: [-180,-90,180,90]
                    crs: http://www.opengis.net/def/crs/OGC/1.3/CRS84
                temporal:
                    begin: 2011-11-11
                    end: null  # or empty (either means open ended)
            provider:
                name: Elasticsearch
                data: http://localhost:9200/dsra_sim6p8_cr2022_rlz_1_b0_economic_loss_agg_view
                id_field: Sauid

> NOTE: a sample configuration is provided in `configuration/local.config.yml`

Install and start PygeoAPI on localhost

    $ . deploy-pygeoapi.sh

Run `load_es_data.py` script passing in a property that you want to use as the `id` (e.g. Sauid)

    $ python scripts/load_es_data.py sample-data/dsra_sim6p8_cr2022_rlz_1_b0_economic_loss_agg_view.geojson Sauid

Check Elasticsearch to ensure that the index was created

    $ http://localhost:9200/_cat/indices?v&pretty

You should see something similar to:

    health status index ...
    green  open   dsra_sim6p8_cr2022_rlz_1_b0_economic_loss_agg_view XnIFL7LNTBWupGSXJOFjig ...

Check PygeoAPI to make sure that the feature collection can be acccesed

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

## Querying PygeoAPI

Refer to the PygeoAPI documentation for general guidance: 

    http://localhost:5000/openapi?f=html

> NOTE: querying is currently limited to spatial extent and exact value queries. For more complex querying use Elasticsearch (see below).

### To filter on a specfic attribute

    http://localhost:5000/collections/economic_loss/items?Magnitude=6.8

## Querying Elasticsearch

### Range query

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

### Specific value

    curl -XGET "http://localhost:9200/dsra_sim6p8_cr2022_rlz_1_b0_economic_loss_agg_view/_search" -H 'Content-Type: 
    application/json' -d'
    {  
        "query": {    
            "match": {      
                "properties.sL_AssetLoss": 1888058    
            }  
        }
    }'
