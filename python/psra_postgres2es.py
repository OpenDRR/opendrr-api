#!/usr/bin/python3
# =================================================================
# SPDX-License-Identifier: MIT
#
# Copyright (C) 2020-2021 Government of Canada
#
# Main Authors: Drew Rotheram <drew.rotheram-clarke@canada.ca>
#               Joost van Ulden <joost.vanulden@canada.ca>
# =================================================================

import utils

def main():
    # building level aggregation
    psraTable = utils.PostGISdataset(
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
        view="opendrr_psra_indicators_b",
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom_point) \
                FROM results_psra_national.psra_indicators_b \
                ORDER BY psra_indicators_b."AssetID" \
                LIMIT {limit} \
                OFFSET {offset}'
    )
    psraTable.postgis2es()

    # Sauid level aggregation
    psraTable = utils.PostGISdataset(
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
        view="opendrr_psra_indicators_s",
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom_poly) \
                    FROM results_psra_national.psra_indicators_s \
                    ORDER BY psra_indicators_s."Sauid" \
                    LIMIT {limit} \
                    OFFSET {offset}'
    )
    psraTable.postgis2es()

    # csd level aggregation
    psraTable = utils.PostGISdataset(
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
        view="opendrr_psra_indicators_csd",
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
                    FROM results_psra_national.psra_indicators_csd \
                    ORDER BY psra_indicators_csd."csduid" \
                    LIMIT {limit} \
                    OFFSET {offset}'
    )
    psraTable.postgis2es()

    # Agg loss
    psraTable = utils.PostGISTable(
        utils.PostGISConnection(),
        utils.ESConnection(settings={
            'settings': {
                'number_of_shards': 1,
                'number_of_replicas': 0
            }
        }),
        view="opendrr_psra_agg_loss_fsa",
        sqlquerystring='SELECT * \
                    FROM results_psra_national.psra_agg_loss_fsa \
                    ORDER BY psra_agg_loss_fsa."fid" \
                    LIMIT {limit} \
                    OFFSET {offset}'
    )
    psraTable.postgis2es()

    # expected loss fsa
    psraTable = utils.PostGISTable(
        utils.PostGISConnection(),
        utils.ESConnection(settings={
            'settings': {
                'number_of_shards': 1,
                'number_of_replicas': 0
            }
        }),
        view="opendrr_psra_expected_loss_fsa",
        sqlquerystring='SELECT * \
                    FROM results_psra_national.psra_expected_loss_fsa \
                    ORDER BY psra_expected_loss_fsa."fid" \
                    LIMIT {limit} \
                    OFFSET {offset}'
    )
    psraTable.postgis2es()
    return

if __name__ == '__main__':
    main()
