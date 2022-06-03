#!/usr/bin/python3

import argparse
import configparser
import decimal
import json
import logging
import os
import sys

import psycopg2
from elasticsearch import Elasticsearch, helpers

"""
Script to convert Hazard_Threat Views to ElasticSearch Index
Can run from the command line with mandatory arguments
Run this script with a command like:
python3 hazardThreat_postgres2es.py
    --type="eq_threat_to_assets"
    --aggregation="sauid"
    --geometry=geom_poly
    --idField="Sauid"
"""


# Main Function
def main():
    logFileName = "{}.log".format(os.path.splitext(sys.argv[0])[0])
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(logFileName), logging.StreamHandler()],
    )
    auth = get_config_params("config.ini")
    args = parse_args()
    view = "nhsl_hazard_threat_{type}_{aggregation}".format(
        **{"type": args.type, "aggregation": args.aggregation[0].lower()}
    )

    if args.idField.lower() == "sauid":
        id_field = "Sauid"
        sqlquerystring = "SELECT *, ST_AsGeoJSON(geom_poly) \
            FROM results_nhsl_hazard_threat.{view}".format(
            **{"view": view}
        )
        settings = {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "geometry": {"type": "geo_shape"},
                    "geom_poly": {"type": "geo_shape"},
                }
            },
        }
    elif args.idField.lower() == "building":
        id_field = "AssetID"
        sqlquerystring = "SELECT *, ST_AsGeoJSON(geom_point) \
            FROM results_nhsl_hazard_threat.{view}".format(
            **{"view": view}
        )
        settings = {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "coordinates": {"type": "geo_point"},
                    "geometry": {"type": "geo_shape"},
                }
            },
        }

    # es=Elasticsearch()
    es = Elasticsearch(
        [auth.get("es", "es_endpoint")],
        http_auth=(auth.get("es", "es_un"), auth.get("es", "es_pw")),
    )
    connection = None
    try:
        # Connect to the PostGIS database hosted on RDS
        connection = psycopg2.connect(
            user=auth.get("rds", "postgres_un"),
            password=auth.get("rds", "postgres_pw"),
            host=auth.get("rds", "postgres_host"),
            port=auth.get("rds", "postgres_port"),
            database=auth.get("rds", "postgres_db"),
        )
        # Query the entire view with the geometries in geojson format
        cur = connection.cursor()
        cur.execute(sqlquerystring)
        rows = cur.fetchall()
        columns = [name[0] for name in cur.description]
        geomIndex = columns.index("st_asgeojson")
        feature_collection = {"type": "FeatureCollection", "features": []}

        # Format the table into a geojson format for ES/Kibana consumption
        for row in rows:
            if args.idField.lower() == "sauid":
                feature = {
                    "type": "Feature",
                    "geometry": json.loads(row[geomIndex]),
                    "properties": {},
                }
                for index, column in enumerate(columns):
                    if column != "st_asgeojson":
                        value = row[index]
                        feature["properties"][column] = value

            elif args.idField.lower() == "building":
                coordinates = json.loads(row[geomIndex])["coordinates"]
                feature = {
                    "type": "Feature",
                    "geometry": json.loads(row[geomIndex]),
                    "coordinates": coordinates,
                    "properties": {},
                }
                for index, column in enumerate(columns):
                    if column != "st_asgeojson":
                        value = row[index]
                        feature["properties"][column] = value

            feature_collection["features"].append(feature)
        geojsonobject = json.dumps(
            feature_collection, indent=2, default=decimal_default
        )

    except (Exception, psycopg2.Error) as error:
        logging.error(error)

    finally:
        if connection:
            # cursor.close()
            connection.close()

    # create index
    if es.indices.exists("opendrr_" + view):
        es.indices.delete("opendrr_" + view)

    es.indices.create(index="opendrr_" + view, body=settings, request_timeout=90)

    d = json.loads(geojsonobject)

    helpers.bulk(es, gendata(d, "opendrr_" + view, id_field), raise_on_error=False)

    return


def gendata(data, view, id_field):
    for item in data["features"]:
        yield {"_index": view, "_id": item["properties"][id_field], "_source": item}


# Function to handle decimal encoder error
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


def get_config_params(args):
    """
    Parse Input/Output columns from supplied *.ini file
    """
    configParseObj = configparser.ConfigParser()
    configParseObj.read(args)
    return configParseObj


def parse_args():
    parser = argparse.ArgumentParser(description="load data PostGIS to ES")
    parser.add_argument(
        "--type",
        type=str,
        help="hazard threat layer (i.e. eq_threat_to_assets)",
        required=True,
    )
    parser.add_argument(
        "--aggregation", type=str, help="building or Sauid", required=True
    )
    parser.add_argument(
        "--geometry", type=str, help="geom_point or geom_poly", required=True
    )
    parser.add_argument(
        "--idField",
        type=str,
        help="Field to use as Index ID. AssetID or Sauid",
        required=True,
    )
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    main()
