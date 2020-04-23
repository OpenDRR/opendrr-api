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

from elasticsearch import Elasticsearch
from elasticsearch import helpers

def get_config_params(args):
    """
    Parse Input/Output columns from supplied *.ini file
    """
    configParseObj = configparser.ConfigParser()
    configParseObj.read(args)
    return configParseObj

auth = get_config_params('config.ini')

es = Elasticsearch([auth.get('es', 'es_endpoint')])



#if len(sys.argv) < 3:
#    print('Usage: {} <path/to/data.geojson> <id-field>'.format(sys.argv[0]))
#    sys.exit(1)

#index_name = os.path.splitext(os.path.basename(sys.argv[1]))[0].lower()
id_field = "Sauid" #sys.argv[2]

view = "dsra_sim6p8_cr2022_rlz_1_r1_economic_loss_agg_view"
index_name = view
#sqlquerystring = 'SELECT * from "results"."{}"'.format(view)
sqlquerystring = 'SELECT *, ST_AsGeoJSON(geom_poly) FROM "results_float"."{}"'.format(view)
connection = None
try:
    connection = psycopg2.connect(user = auth.get('rds', 'postgres_un'),
                                    password = auth.get('rds', 'postgres_pw'),
                                    host = auth.get('rds', 'postgres_host'),
                                    port = auth.get('rds', 'postgres_port'),
                                    database = auth.get('rds', 'postgres_db'))
    #pgdf = geopandas.GeoDataFrame.from_postgis(sqlquerystring, connection, geom_col='geom_poly')
    #pgdf.to_file(view+'.json', driver="GeoJSON")
    cur = connection.cursor()
    cur.execute(sqlquerystring)
    rows = cur.fetchall()
    columns = [name[0] for name in cur.description]
    geomIndex = columns.index('st_asgeojson')

    feature_collection = {'type': 'FeatureCollection', 'features': []}

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

    geojsonobject = json.dumps(feature_collection, indent=2)#, default=)

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)

finally:
    if(connection):
        # cursor.close()
        connection.close()


if es.indices.exists(index_name):
    es.indices.delete(index_name)

# index settings
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

# create index
es.indices.create(index=index_name, body=settings, request_timeout=90)

#with open(sys.argv[1]) as fh:
#    d = json.load(fh)
d = json.loads(geojsonobject)

def gendata(data):
    for item in data['features']:
        yield {
            "_index": index_name,
            "_id": item['properties'][id_field],
            "_source": item
        }



helpers.bulk(es, gendata(d), raise_on_error=False)