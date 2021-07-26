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
        view = "sauid_hexgrid",
        sqlquerystring = 'SELECT *, ST_AsGeoJSON(geom) \
                    FROM boundaries."SAUID_HexGrid" \
                    ORDER BY "SAUID_HexGrid"."sauid" \
                    LIMIT {limit} \
                    OFFSET {offset}'
    )

    table.postgis2es()

    return

if __name__ == '__main__':
    main()