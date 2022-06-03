#!/usr/bin/python3
# =================================================================
# SPDX-License-Identifier: MIT
#
# Copyright (C) 2020-2021 Government of Canada
#
# Main Authors: Drew Rotheram <drew.rotheram-clarke@canada.ca>
#               Joost van Ulden <joost.vanulden@canada.ca>
# =================================================================

import argparse

import utils


def main():
    config = utils.get_config_params("config.ini")
    version = config.get("es", "version")

    # args = parse_args()
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "properties": {
                        "coordinates": {"type": "geo_point"},
                        "geometry": {"type": "geo_shape"},
                    }
                },
            }
        ),
        view="opendrr_psra_uhs_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
                FROM results_psra_national.psra_uhs \
                ORDER BY psra_uhs."geom" \
                LIMIT {limit} \
                OFFSET {offset}',
    )

    table.postgis2es()

    return


def parse_args():
    parser = argparse.ArgumentParser(description="script description")
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
