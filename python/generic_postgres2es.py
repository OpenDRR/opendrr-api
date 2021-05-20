# =================================================================
#!/bin/bash
# SPDX-License-Identifier: MIT
#
# Copyright (C) 2020-2021 Government of Canada
#
# Main Authors: Drew Rotheram <drew.rotheram-clarke@canada.ca>
#               Joost van Ulden <joost.vanulden@canada.ca>
# python3 generic_postgres2es.py --sqlquerystring='SELECT *, ST_AsGeoJSON(geom) FROM dsra.dsra_all_scenarios_dauid ORDER BY dsra_all_scenarios_dauid."dauid" LIMIT {limit} OFFSET {offset}' --view='dsra_all_scenarios_dauid' 
# =================================================================


import utils
import json
import argparse

def main():
    args = parse_args()
    settings = json.dumps(json.load(open(args.esSettings)))
    mappings = json.dumps(json.load(open(args.esMappings)))
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(settings = {
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
        } ),
        view = args.view,
        sqlquerystring = args.sqlquerystring
    )

    table.postgis2es()


    return

def parse_args():
    parser = argparse.ArgumentParser(description="script to load any view from PostGIS to ElasticSearch using ")
    parser.add_argument("--sqlquerystring",
                        type=str,
                        help="Full SQL query. Remember to include a SORT BY and LIMIT OFFSET",
                        required=True)
    parser.add_argument("--view",
                        type=str,
                        help="Name of the ElasticSearch index",
                        required=True)
    parser.add_argument("--esSettings",
                        type=str,
                        default="esSettings.json",
                        help="ElasticSearch Settings File",
                        required=False)
    parser.add_argument("--esMappings",
                        type=str,
                        default="sauidESMappings.json",
                        help="ElasticSearch Mappings File",
                        required=False)
    args = parser.parse_args()

    return args
if __name__ == '__main__':
    main()