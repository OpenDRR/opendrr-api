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

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from decimal import Decimal
from tqdm import tqdm

'''
Script to convert Physical Exposure Views to ElasticSearch Index
Can be run from the command line with mandatory arguments 
Run this script with a command like:
python3 exposure_postgres2es.py --type="buildings" --aggregation="building" --geometry=geom_point --idField="AssetID"
'''

#Main Function
def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s', 
                        handlers=[logging.FileHandler('{}.log'.format(os.path.splitext(sys.argv[0])[0])),
                                  logging.StreamHandler()])
    tracer = logging.getLogger('elasticsearch')
    tracer.setLevel(logging.ERROR)
    tracer.addHandler(logging.FileHandler('{}.log'.format(os.path.splitext(sys.argv[0])[0])))
    auth = get_config_params('config.ini')
    args = parse_args()

    if args.geometry == "geom_point":
        geoType = "geo_point"
    elif args.geometry =="geom_poly":
        geoType = "geo_shape"
    else:
        raise Exception("Unrecognized Geometry Type")

    # index settings
    settings = {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0
        },
        'mappings': {
            'properties': {
                'geometry': {
                    'type': '{}'.format(geoType)
                }
            }
        }
    }

    view = "canada_exposure_{type}_{aggregation}".format(**{'type':args.type, 'aggregation':args.aggregation})
    id_field = args.idField    
    
    es = Elasticsearch()
    #es = Elasticsearch([auth.get('es', 'es_endpoint')], http_auth=(auth.get('es', 'es_un'), auth.get('es', 'es_pw')))
    # create index
    if es.indices.exists(view):
        es.indices.delete(view)
    es.indices.create(index=view, body=settings, request_timeout=90)

    sqlquerystring = 'SELECT *, ST_AsGeoJSON({geometry}) FROM "results_canada_exposure"."{view}"'.format(**{'geometry':args.geometry, 'view':view})
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
        i = 0
        for row in tqdm(rows):
            feature = {
                'type': 'Feature',
                'geometry': json.loads(row[geomIndex]),
                'properties': {},
            }
            for index, column in enumerate(columns):
                if column != "st_asgeojson":
                    value =row[index]
                    if isinstance(value, Decimal):
                        value = float(value)
                    feature['properties'][column] = value
            feature_collection['features'].append(feature)    
            i+=1
            if i==10000:
                geojsonobject = json.dumps(feature_collection, indent=2)
                d = json.loads(geojsonobject)
                helpers.bulk(es, gendata(d, view, id_field), raise_on_error=False, request_timeout=30)
                feature_collection = {'type': 'FeatureCollection', 'features': []}
                i=0
        geojsonobject = json.dumps(feature_collection, indent=2)
        d = json.loads(geojsonobject)
        helpers.bulk(es, gendata(d, view, id_field), raise_on_error=False, request_timeout=30)
        feature_collection = {'type': 'FeatureCollection', 'features': []}

    except (Exception, psycopg2.Error) as error :
        logging.error(error)

    finally:
        if(connection):
            # cursor.close()
            connection.close()
    return

def gendata(data, view, id_field):
    for item in data['features']:
        yield {
            "_index": view,
            "_id": item['properties'][id_field],
            "_source": item
        }

def get_config_params(args):
    """
    Parse Input/Output columns from supplied *.ini file
    """
    configParseObj = configparser.ConfigParser()
    configParseObj.read(args)
    return configParseObj

def parse_args():
    parser = argparse.ArgumentParser(description="load exposure data from PostGIS to ElasticSearch Index")
    parser.add_argument("--type", type=str, help="buildings or people", required=True)
    parser.add_argument("--aggregation", type=str, help="building or Sauid", required=True)
    parser.add_argument("--geometry", type=str, help="geom_point or geom_poly", required=True)
    parser.add_argument("--idField", type=str, help="Field to use as ElasticSearch Index ID. AssetID or Sauid", required=True)
    args = parser.parse_args()
    
    return args

if __name__ == '__main__':
    main() 