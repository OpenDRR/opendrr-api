# =================================================================
#!/bin/bash
# SPDX-License-Identifier: MIT
#
# Copyright (C) 2020-2021 Government of Canada
#
# Main Authors: Drew Rotheram <drew.rotheram-clarke@canada.ca>
#               Joost van Ulden <joost.vanulden@canada.ca>
# =================================================================

import utils 
import argparse


'''
Script to convert Physical Exposure Views to ElasticSearch Index
Can be run from the command line with mandatory arguments
Run this script with a command like:
python3 exposure_postgres2es.py
    --type="assets"
    --aggregation="building"
    --geometry=geom_point
    --idField="BldgID"
'''

#Main Function
def main():
    args = parse_args()

    # index settings
    if args.geometry == "geom_poly":
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
            view = "opendrr_nhsl_physical_exposure_all_indicators_{aggregation}".format(**{
                    'aggregation': args.aggregation[0].lower()}),
            sqlquerystring =  'SELECT *, ST_AsGeoJSON(geom_poly) \
                    FROM results_nhsl_physical_exposure.nhsl_physical_exposure_all_indicators_{aggregation} \
                    LIMIT {{limit}} \
                    OFFSET {{offset}}'.format(**{
                    'aggregation': args.aggregation[0].lower()})
        )

    elif args.geometry == "geom_point":
        table = utils.PostGISdataset(
            utils.PostGISConnection(),
            utils.ESConnection(settings = {
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
            } ), 
            view = "opendrr_nhsl_physical_exposure_all_indicators_{aggregation}".format(**{
                    'aggregation': args.aggregation[0].lower()}),
            sqlquerystring = 'SELECT *, ST_AsGeoJSON(geom_point) \
                    FROM results_nhsl_physical_exposure.nhsl_physical_exposure_all_indicators_{aggregation} \
                    LIMIT {{limit}} \
                    OFFSET {{offset}}'.format(**{
                    'aggregation': args.aggregation[0].lower()})
        )

    table.postgis2es()

    return 


def parse_args():
    parser = argparse.ArgumentParser(description="load exposure PostGIS to ES")
    # parser.add_argument("--type",
    #                     type=str,
    #                     help="assets building(s) or people",
    #                     required=True)
    parser.add_argument("--aggregation",
                        type=str,
                        help="building or Sauid",
                        required=True)
    parser.add_argument("--geometry",
                        type=str,
                        help="geom_point or geom_poly",
                        required=True)
    # parser.add_argument("--idField",
    #                     type=str,
    #                     help="Field to use as Index ID. AssetID or Sauid",
    #                     required=True)
    args = parser.parse_args()
    
    return args

if __name__ == '__main__':
    main() 