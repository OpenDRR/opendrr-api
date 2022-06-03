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
    config = utils.get_config_params("config.ini")
    version = config.get("es", "version")

    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_psra_hmaps_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
                    FROM results_psra_national."psra_hmaps" \
                    ORDER BY "psra_hmaps"."geom" \
                    LIMIT {limit} \
                    OFFSET {offset}',
    )
    table.postgis2es()

    return


if __name__ == "__main__":
    main()
