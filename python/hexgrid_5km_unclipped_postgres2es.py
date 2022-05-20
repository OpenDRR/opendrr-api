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
    table = utils.PostGISdataset(
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
        view="opendrr_hexgrid_5km_unclipped",
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
                    FROM boundaries."HexGrid_5km_unclipped" \
                    ORDER BY "HexGrid_5km_unclipped"."gridid_5" \
                    LIMIT {limit} \
                    OFFSET {offset}'
    )

    table.postgis2es()

    return


if __name__ == '__main__':
    main()
