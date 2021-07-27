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

def main():
    psraTable = utils.PostGISdataset(
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
        view = "psra_all_indicators_s",
        sqlquerystring = 'SELECT *, ST_AsGeoJSON(geom_poly) \
                    FROM results_psra_national.psra_all_indicators_s \
                    ORDER BY psra_all_indicators_s."Sauid" \
                    LIMIT {limit} \
                    OFFSET {offset}'
    )

    psraTable.postgis2es()

    psraTable = utils.PostGISdataset(
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
            view = "psra_all_indicators_b",
            sqlquerystring = 'SELECT *, ST_AsGeoJSON(geom_point) \
                    FROM results_psra_national.psra_all_indicators_b \
                    ORDER BY psra_all_indicators_b."AssetID" \
                    LIMIT {limit} \
                    OFFSET {offset}'
    )

    psraTable.postgis2es()

    return

if __name__ == '__main__':
    main()