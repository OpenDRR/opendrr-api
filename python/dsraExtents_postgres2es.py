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

    dsraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_shakemap_scenario_extents_{}".format(version),
        sqlquerystring="SELECT *, ST_AsGeoJSON(geom) \
                    FROM gmf.shakemap_scenario_extents \
                    LIMIT {limit} \
                    OFFSET {offset}",
    )
    dsraTable.postgis2es()
    return


def parse_args():
    parser = argparse.ArgumentParser(
        description="""
        Create ES index with DSRA Scenario Extents"""
    )
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    main()
