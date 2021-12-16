# =================================================================
# !/bin/bash
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

    # Create building level aggregation object and load to ES
    dsraTable = utils.PostGISPointDataset(
        utils.PostGISConnection(),
        utils.ESConnection(settings={
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
        }),
        view = "opendrr_dsra_{eqScenario}_indicators_b".format(**{
            'eqScenario': args.eqScenario}).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom_point) \
                    FROM results_dsra_{eqScenario}.dsra_{eqScenario}_indicators_b \
                    ORDER BY dsra_{eqScenario}_indicators_b."AssetID" \
                    LIMIT {{limit}} \
                    OFFSET {{offset}}'.format(**{
                        'eqScenario': args.eqScenario})
    )
    dsraTable.postgis2es()

    # Create Sauid level aggregation object and load to ES
    dsraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(settings={
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
        }),
        view = "opendrr_dsra_{eqScenario}_indicators_s".format(**{
            'eqScenario': args.eqScenario}).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom_poly) \
                    FROM results_dsra_{eqScenario}.dsra_{eqScenario}_indicators_s \
                    ORDER BY dsra_{eqScenario}_indicators_s."Sauid" \
                    LIMIT {{limit}} \
                    OFFSET {{offset}}'.format(**{
                        'eqScenario': args.eqScenario})
    )
    dsraTable.postgis2es()

    # Create CSD level aggregation object and load to ES
    # dsraTable = utils.PostGISdataset(
    #     utils.PostGISConnection(),
    #     utils.ESConnection(settings={
    #         'settings': {
    #             'number_of_shards': 1,
    #             'number_of_replicas': 0
    #         },
    #         'mappings': {
    #             'properties': {
    #                 'geometry': {
    #                     'type': 'geo_shape'
    #                 }
    #             }
    #         }
    #     }),
    #     view = "opendrr_dsra_{eqScenario}_indicators_csd".format(**{
    #         'eqScenario': args.eqScenario}).lower(),
    #     sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
    #                 FROM results_dsra_{eqScenario}.dsra_{eqScenario}_indicators_csd \
    #                 ORDER BY dsra_{eqScenario}_indicators_csd."csduid" \
    #                 LIMIT {{limit}} \
    #                 OFFSET {{offset}}'.format(**{
    #                     'eqScenario': args.eqScenario})
    # )
    # dsraTable.postgis2es()

    # Create shakemap object and load to ES
    dsraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(settings={
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
        }),
        view = "opendrr_dsra_{eqScenario}_shakemap".format(**{
            'eqScenario': args.eqScenario}).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
                    FROM results_dsra_{eqScenario}.dsra_{eqScenario}_shakemap \
                    ORDER BY dsra_{eqScenario}_shakemap."SiteID" \
                    LIMIT {{limit}} \
                    OFFSET {{offset}}'.format(**{
                        'eqScenario': args.eqScenario})
    )
    dsraTable.postgis2es()
    return

def parse_args():
    parser = argparse.ArgumentParser(description="script description")
    parser.add_argument("--eqScenario", type=str, help="Earthquake scenario id", required=True)
    args = parser.parse_args()

    return args

if __name__ == '__main__':
    main()