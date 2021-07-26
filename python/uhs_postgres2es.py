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

def main():
    args = parse_args()
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
        view="psra_{province}_uhs".format(**{
            'province': args.province.lower()}),
        sqlquerystring = 'SELECT *, ST_AsGeoJSON(geom) \
                FROM results_psra_{province}.psra_{province}_uhs \
                ORDER BY psra_{province}_uhs."geom" \
                LIMIT {{limit}} \
                OFFSET {{offset}}'.format(**{'province': args.province.lower()})
    )

    table.postgis2es()

    return

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