# =================================================================

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
Script to convert PSRA indicator views to ElasticSearch Index
Can be run from the command line with mandatory arguments
Run this script with a command like:
python3 psra_postgres2es.py
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
    view = "psra_{province}_{dbview}_{idField}".format(**{
        'province': args.province,
        'dbview': args.dbview,
        'idField': args.idField[0]}).lower()
    if args.idField == 'sauid':
        id_field = 'Sauid'
        sqlquerystring = 'SELECT *, ST_AsGeoJSON(geom_poly) \
            FROM results_psra_{province}.{view}'.format(**{
            'province': args.province,
            'view': view})
        settings = {
            'settings': {
                'number_of_shards': 1,
                'number_of_replicas': 0
            },
            'mappings': {
                'properties': {
                    'geometry': {
                        'type': 'geo_shape'
                    }
                }
            }
        }

    elif args.idField == 'building':
        id_field = 'AssetID'
        sqlquerystring = 'SELECT *, ST_AsGeoJSON(geom_point) \
            FROM results_psra_{province}.{view}'.format(**{
            'province': args.province,
            'view': view})
        settings = {
            'settings': {
                'number_of_shards': 1,
                'number_of_replicas': 0
            },
            'mappings': {
                'properties': {
                    'geometry': {
                        'properties': {
                            'coordinates': {
                                'type': 'geo_point'
                            }
                        }
                    }
                }
            }
        }

    # es = Elasticsearch()
    es = Elasticsearch([auth.get('es', 'es_endpoint')],
                        http_auth=(auth.get('es', 'es_un'),
                                    auth.get('es', 'es_pw')))
    connection = None
    try:
        # Connect to the PostGIS database hosted on RDS
        connection = psycopg2.connect(user=auth.get('rds', 'postgres_un'),
                                      password=auth.get('rds','postgres_pw'),
                                      host=auth.get('rds', 'postgres_host'),
                                      port=auth.get('rds', 'postgres_port'),
                                      database=auth.get('rds', 'postgres_db'))
        # Query the entire view with the geometries in geojson format
        cur = connection.cursor()
        cur.execute(sqlquerystring)
        rows = cur.fetchall()
        columns = [name[0] for name in cur.description]
        geomIndex = columns.index('st_asgeojson')
        feature_collection = {'type': 'FeatureCollection', 'features': []}

        # Format the table into a geojson format for ES/Kibana consumption
        for row in rows:
            feature = {
                'type': 'Feature',
                'geometry': json.loads(row[geomIndex]),
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

    except (Exception, psycopg2.Error) as error:
        logging.error(error)

    finally:
        if(connection):
            # cursor.close()
            connection.close()

    # create index
    if es.indices.exists(view):
        es.indices.delete(view)

    es.indices.create(index=view, body=settings, request_timeout=90)

    d = json.loads(geojsonobject)

    helpers.bulk(es, gendata(d, view, id_field), raise_on_error=False)

    return


def gendata(data, view, id_field):
    for item in data['features']:
        yield {
            "_index": view,
            "_id": item['properties'][id_field],
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
    parser.add_argument("--dbview",
                        type=str,
                        help=" Thematic Database View",
                        required=True)
    parser.add_argument("--idField",
                        type=str,
                        help="Field to use as ElasticSearch Index ID",
                        required=True)
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    main()
