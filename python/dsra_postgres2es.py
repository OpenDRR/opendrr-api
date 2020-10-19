# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2020 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
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
Script to convert DSRA indicator views to ElasticSearch Index
Can be run from the command line with mandatory arguments 
Run this script with a command like:
python3 dsra_postgres2es.py --eqScenario="sim6p8_cr2022_rlz_1" --dbview=casualties_agg_view --idField="Sauid"
'''

#Main Function
def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s', 
                        handlers=[logging.FileHandler('{}.log'.format(os.path.splitext(sys.argv[0])[0])),
                                  logging.StreamHandler()])
    auth = get_config_params('config.ini')
    args = parse_args()
    view = "{eq_scenario}_{dbview}_{idField}".format(**{
        'eq_scenario': args.eqScenario,
        'dbview': args.dbview,
        'idField': args.idField}).lower()
    if args.idField == 'sauid':
        id_field = 'Sauid'
        sqlquerystring = 'SELECT *, ST_AsGeoJSON(geom_poly) \
            FROM results_{eqScenario}.{view}'.format(**{
            'eqScenario': args.eqScenario,
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
            FROM results_{eqScenario}.{view}'.format(**{
            'eqScenario': args.eqScenario,
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
                            'coordinates':{
                                'type': 'geo_point'
                            }    
                        }
                    }
                }
            }
        }

    #es = Elasticsearch()
    es = Elasticsearch([auth.get('es', 'es_endpoint')], http_auth=(auth.get('es', 'es_un'), auth.get('es', 'es_pw')))
    connection = None
    try:
        #Connect to the PostGIS database hosted on RDS
        connection = psycopg2.connect(user = auth.get('rds', 'postgres_un'),
                                        password = auth.get('rds', 'postgres_pw'),
                                        host = auth.get('rds', 'postgres_host'),
                                        port = auth.get('rds', 'postgres_port'),
                                        database = auth.get('rds', 'postgres_db'))
        #Query the entire view with the geometries in geojson format
        cur = connection.cursor()
        cur.execute(sqlquerystring)
        rows = cur.fetchall()
        columns = [name[0] for name in cur.description]
        geomIndex = columns.index('st_asgeojson')
        feature_collection = {'type': 'FeatureCollection', 'features': []}
        
        #Format the table into a geojson format for ES/Kibana consumption
        for row in rows:
            feature = {
                'type': 'Feature',
                'geometry': json.loads(row[geomIndex]),
                'properties': {},
            }
            for index, column in enumerate(columns):
                if column != "st_asgeojson":
                    value =row[index]
                    feature['properties'][column] = value
            feature_collection['features'].append(feature)
        geojsonobject = json.dumps(feature_collection, indent=2, default=decimal_default)

    except (Exception, psycopg2.Error) as error :
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

#Function to handle decimal encoder error
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
    parser.add_argument("--eqScenario", type=str, help="Earthquake scenario id", required=True)
    parser.add_argument("--dbview", type=str, help=" Thematic Database View. Allowable values: (casualties, damage_state, economic_loss, functional_state, recovery, scenario_hazard, scenario_hazard_threat,scenario_rupture, social_disruption)", required=True)
    parser.add_argument("--idField", type=str, help="Field to use as ElasticSearch Index ID", required=True)
    args = parser.parse_args()
    
    return args

if __name__ == '__main__':
    main() 