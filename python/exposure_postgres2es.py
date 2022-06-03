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

"""
Script to convert Physical Exposure Views to ElasticSearch Index
Can be run from the command line with mandatory arguments
Run this script with a command like:
python3 exposure_postgres2es.py
    --type="assets"
    --aggregation="building"
    --geometry=geom_point
    --idField="BldgID"
"""


# Main Function
def main():

    config = utils.get_config_params("config.ini")
    version = config.get("es", "version")

    # sauid level aggregation
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_nhsl_physical_exposure_indicators_s_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom_poly) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_s \
            ORDER BY nhsl_physical_exposure_indicators_s."Sauid" \
            LIMIT {limit} \
            OFFSET {offset}',
    )
    table.postgis2es()

    # building level aggregation
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
        view="opendrr_nhsl_physical_exposure_indicators_b_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom_point) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_b \
            ORDER BY nhsl_physical_exposure_indicators_b."BldgID" \
            LIMIT {limit} \
            OFFSET {offset}',
    )
    table.postgis2es()

    # hexgrid 1km aggregation
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_nhsl_physical_exposure_indicators_hexgrid_1km_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexgrid_1km \
            ORDER BY nhsl_physical_exposure_indicators_hexgrid_1km."gridid_1" \
            LIMIT {limit} \
            OFFSET {offset}',
    )
    table.postgis2es()

    # hexgrid 1km unclipped aggregation
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_nhsl_physical_exposure_indicators_hexgrid_1km_uc_{}".format(
            version
        ),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexgrid_1km_uc \
            ORDER BY nhsl_physical_exposure_indicators_hexgrid_1km_uc."gridid_1" \
            LIMIT {limit} \
            OFFSET {offset}',
    )
    table.postgis2es()

    # hexgrid 5km aggregation
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_nhsl_physical_exposure_indicators_hexgrid_5km_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexgrid_5km \
            ORDER BY nhsl_physical_exposure_indicators_hexgrid_5km."gridid_5" \
            LIMIT {limit} \
            OFFSET {offset}',
    )
    table.postgis2es()

    # hexgrid 5km unclipped aggregation
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_nhsl_physical_exposure_indicators_hexgrid_5km_uc_{}".format(
            version
        ),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexgrid_5km_uc \
            ORDER BY nhsl_physical_exposure_indicators_hexgrid_5km_uc."gridid_5" \
            LIMIT {limit} \
            OFFSET {offset}',
    )
    table.postgis2es()

    # hexgrid 10km aggregation
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_nhsl_physical_exposure_indicators_hexgrid_10km_{}".format(
            version
        ),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexgrid_10km \
            ORDER BY nhsl_physical_exposure_indicators_hexgrid_10km."gridid_10" \
            LIMIT {limit} \
            OFFSET {offset}',
    )
    table.postgis2es()

    # hexgrid 10km unclipped aggregation
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_nhsl_physical_exposure_indicators_hexgrid_10km_uc_{}".format(
            version
        ),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexgrid_10km_uc \
            ORDER BY nhsl_physical_exposure_indicators_hexgrid_10km_uc."gridid_10" \
            LIMIT {limit} \
            OFFSET {offset}',
    )
    table.postgis2es()

    # hexgrid 25km aggregation
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_nhsl_physical_exposure_indicators_hexgrid_25km_{}".format(
            version
        ),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexgrid_25km \
            ORDER BY nhsl_physical_exposure_indicators_hexgrid_25km."gridid_25" \
            LIMIT {limit} \
            OFFSET {offset}',
    )
    table.postgis2es()

    # hexgrid 25km  unclipped aggregation
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_nhsl_physical_exposure_indicators_hexgrid_25km_uc_{}".format(
            version
        ),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexgrid_25km_uc \
            ORDER BY nhsl_physical_exposure_indicators_hexgrid_25km_uc."gridid_25" \
            LIMIT {limit} \
            OFFSET {offset}',
    )
    table.postgis2es()

    # hexgrid 50km unclipped aggregation
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_nhsl_physical_exposure_indicators_hexgrid_50km_uc_{}".format(
            version
        ),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexgrid_50km_uc \
            ORDER BY nhsl_physical_exposure_indicators_hexgrid_50km_uc."gridid_50" \
            LIMIT {limit} \
            OFFSET {offset}',
    )
    table.postgis2es()

    # hexgrid 100km unclipped aggregation
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_nhsl_physical_exposure_indicators_hexgrid_100km_uc_{}".format(
            version
        ),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexgrid_100km_uc \
            ORDER BY nhsl_physical_exposure_indicators_hexgrid_100km_uc."gridid_100" \
            LIMIT {limit} \
            OFFSET {offset}',
    )
    table.postgis2es()

    # hexgrid global fabric
    # table = utils.PostGISdataset(
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
    #     view="opendrr_nhsl_physical_exposure_indicators_hexgrid_global_fabric",
    #     sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
    #         FROM \
    #         results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexgrid_global_fabric \
    #         ORDER BY nhsl_physical_exposure_indicators_hexgrid_global_fabric."gridid" \
    #         LIMIT {limit} \
    #         OFFSET {offset}'
    # )
    # table.postgis2es()

    return


def parse_args():
    parser = argparse.ArgumentParser(description="load exposure PostGIS to ES")
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    main()
