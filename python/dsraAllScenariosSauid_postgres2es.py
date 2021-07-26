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
from utils import ESConnection
from utils import PostGISdataset
from utils import PostGISConnection


def main():
    table = PostGISdataset(
        PostGISConnection(),
        ESConnection(settings = {
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
        view = "dsra_all_scenarios_sauid",
        sqlquerystring = 'SELECT *, ST_AsGeoJSON(geom) \
                    FROM dsra.dsra_all_scenarios_sauid \
                    ORDER BY dsra_all_scenarios_sauid."sauid" \
                    LIMIT {limit} \
                    OFFSET {offset}'
    )

    table.postgis2es()

    return

if __name__ == '__main__':
    main()