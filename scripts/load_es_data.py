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

from elasticsearch import Elasticsearch
es = Elasticsearch()

if len(sys.argv) < 3:
    print('Usage: {} <path/to/data.geojson> <id-field>'.format(sys.argv[0]))
    sys.exit(1)

index_name = os.path.splitext(os.path.basename(sys.argv[1]))[0].lower()
id_field = sys.argv[2]

if es.indices.exists(index_name):
    es.indices.delete(index_name)

def flatten_json(input_json):
    out = {}
    for field in input_json:
        if field == 'properties':
            for subfield in input_json['properties']:
                out[subfield] = input_json['properties'][subfield]
        elif field =='geometry':
            out['coordinates'] = input_json['geometry']
            
    return out


# index settings
settings = {
    'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0
    },
    "mappings": {
        'properties': {
            'coordinates': {
                'type': 'geo_shape'
            }
        }
    }
}


# create index
es.indices.create(index=index_name, body=settings, request_timeout=90)

with open(sys.argv[1]) as fh:
    d = json.load(fh)

for fRaw in d['features']:
    f = flatten_json(fRaw)
    try:
        f[id_field] = int(f[id_field])
    except ValueError:
        f[id_field] = f[id_field]
    try:
        res = es.index(index=index_name, id=f[id_field], body=f)
    except:
        print("Sauid: "+str(f[id_field])+" not loaded correctly")