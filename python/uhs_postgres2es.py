# =================================================================
#!/bin/bash
# SPDX-License-Identifier: MIT
#
# Copyright (C) 2020-2021 Government of Canada
#
# Main Authors: Drew Rotheram <drew.rotheram-clarke@canada.ca>
#               Joost van Ulden <joost.vanulden@canada.ca>
# =================================================================

import json
import os
import sys
import psycopg2
import configparser
import logging
import argparse
import decimal

from elasticsearch import Elasticsearch
from elasticsearch import helpers

'''
Script to convert uhs views to ElasticSearch Index
Can be run from the command line with mandatory arguments
Run this script with a command like:
python3 uhs_postgres2es.py --province=${PT}
'''


# Main Function
def main():
    logFileName = '{}.log'.format(os.path.splitext(sys.argv[0])[0])
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(logFileName),
                                  logging.StreamHandler()])
    auth = get_config_params('config.ini')
    args = parse_args()
    view = "psra_{province}_uhs".format(**{
        'province': args.province.lower()})
    limit = 10000
    offset = 0

    # create index
    es = Elasticsearch([auth.get('es', 'es_endpoint')],
                       http_auth=(auth.get('es', 'es_un'),
                       auth.get('es', 'es_pw')))
    if es.indices.exists(view):
        es.indices.delete(view)

    # id_field = 'AssetID'
    settings = {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0
        },
        'mappings': {
            'properties': {
                'coordinates': {
                    'type': 'geo_point'
                },
                'geometry': {
                    'type': 'geo_shape'
                }
            }
        }
    }
    es.indices.create(index=view, body=settings, request_timeout=90)

    while True:
        sqlquerystring = 'SELECT *, ST_AsGeoJSON(geom) \
            FROM results_psra_{province}.{view} \
            ORDER BY {view}."geom" \
            LIMIT {limit} \
            OFFSET {offset}'.format(**{'province': args.province.lower(),
                                       'view': view,
                                       'limit': limit,
                                       'offset': offset})
        offset += limit
        connection = None
        try:
            # Connect to the PostGIS database
            connection = psycopg2.connect(user=auth.get('rds',
                                                        'postgres_un'),
                                          password=auth.get('rds',
                                                            'postgres_pw'),
                                          host=auth.get('rds',
                                                        'postgres_host'),
                                          port=auth.get('rds',
                                                        'postgres_port'),
                                          database=auth.get('rds',
                                                            'postgres_db'))
            # Query the entire view with the geometries in geojson format
            cur = connection.cursor()
            cur.execute(sqlquerystring)
            rows = cur.fetchall()
            if rows:
                columns = [name[0] for name in cur.description]
                geomIndex = columns.index('st_asgeojson')
                feature_collection = {'type': 'FeatureCollection',
                                      'features': []}
                # Format table into a geojson format for ES/Kibana consumption
                for row in rows:
                    coordinates = json.loads(row[geomIndex])['coordinates']
                    feature = {
                        'type': 'Feature',
                        'geometry': json.loads(row[geomIndex]),
                        'coordinates': coordinates,
                        'properties': {},
                    }
                    for index, column in enumerate(columns):
                        if column != "st_asgeojson":
                            value = row[index]
                            feature['properties'][column] = value

                    feature_collection['features'].append(feature)
                geojsonobject = json.dumps(feature_collection,
                                           indent=2,
                                           default=decimal_default)
                d = json.loads(geojsonobject)
                helpers.bulk(es,
                             gendata(d, view),
                             raise_on_error=False)

            else:
                if(connection):
                    connection.close()
                return

        except (Exception, psycopg2.Error) as error:
            logging.error(error)


def gendata(data, view):
    for item in data['features']:
        yield {
            "_index": view,
            # "_id": item['properties'][id_field],
            "_source": item
        }


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
    parser = argparse.ArgumentParser(description="script description")
    parser.add_argument("--province",
                        type=str,
                        help="Two letters only",
                        required=True)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()
