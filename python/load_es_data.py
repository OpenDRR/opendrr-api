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

from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch()

if len(sys.argv) < 3:
    print('Usage: {} <path/to/data.geojson> <id-field>'.format(sys.argv[0]))
    sys.exit(1)

index_name = os.path.splitext(os.path.basename(sys.argv[1]))[0].lower()
id_field = sys.argv[2]

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

with open(sys.argv[1]) as fh:
    d = json.load(fh)

def gendata(data):
    for item in data['features']:
        yield {
            "_index": index_name,
            "_id": item['properties'][id_field],
            "_source": item
        }

helpers.bulk(es, gendata(d), raise_on_error=False)