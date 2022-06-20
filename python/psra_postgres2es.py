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

config = utils.get_config_params("config.ini")
version = config.get("es", "version")


def main():
    # building level aggregation
    psraTable = utils.PostGISdataset(
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
        view="opendrr_psra_indicators_b_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom_point) \
                FROM results_psra_national.psra_indicators_b \
                ORDER BY psra_indicators_b."AssetID" \
                LIMIT {limit} \
                OFFSET {offset}',
    )
    psraTable.postgis2es()

    # Sauid level aggregation
    psraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_psra_indicators_s_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom_poly) \
                    FROM results_psra_national.psra_indicators_s \
                    ORDER BY psra_indicators_s."Sauid" \
                    LIMIT {limit} \
                    OFFSET {offset}',
    )
    psraTable.postgis2es()

    # csd level aggregation
    psraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_psra_indicators_csd_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
                    FROM results_psra_national.psra_indicators_csd \
                    ORDER BY psra_indicators_csd."csduid" \
                    LIMIT {limit} \
                    OFFSET {offset}',
    )
    psraTable.postgis2es()

    # Agg loss
    psraTable = utils.PostGISTable(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={"settings": {"number_of_shards": 1, "number_of_replicas": 0}}
        ),
        view="opendrr_psra_agg_loss_fsa_{}".format(version),
        sqlquerystring='SELECT * \
                    FROM results_psra_national.psra_agg_loss_fsa \
                    ORDER BY psra_agg_loss_fsa."fid" \
                    LIMIT {limit} \
                    OFFSET {offset}',
    )
    psraTable.postgis2es()

    # expected loss fsa
    psraTable = utils.PostGISTable(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={"settings": {"number_of_shards": 1, "number_of_replicas": 0}}
        ),
        view="opendrr_psra_expected_loss_fsa_{}".format(version),
        sqlquerystring='SELECT * \
                    FROM results_psra_national.psra_expected_loss_fsa \
                    ORDER BY psra_expected_loss_fsa."fid" \
                    LIMIT {limit} \
                    OFFSET {offset}',
    )
    psraTable.postgis2es()

    # hexgrid 1km aggregation
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_psra_indicators_hexgrid_1km_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_psra_national.psra_indicators_hexgrid_1km \
            ORDER BY psra_indicators_hexgrid_1km."gridid_1" \
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
        view="opendrr_psra_indicators_hexgrid_1km_uc_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_psra_national.psra_indicators_hexgrid_1km_uc \
            ORDER BY psra_indicators_hexgrid_1km_uc."gridid_1" \
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
        view="opendrr_psra_indicators_hexgrid_5km_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_psra_national.psra_indicators_hexgrid_5km \
            ORDER BY psra_indicators_hexgrid_5km."gridid_5" \
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
        view="opendrr_psra_indicators_hexgrid_5km_uc_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_psra_national.psra_indicators_hexgrid_5km_uc \
            ORDER BY psra_indicators_hexgrid_5km_uc."gridid_5" \
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
        view="opendrr_psra_indicators_hexgrid_10km_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_psra_national.psra_indicators_hexgrid_10km \
            ORDER BY psra_indicators_hexgrid_10km."gridid_10" \
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
        view="opendrr_psra_indicators_hexgrid_10km_uc_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_psra_national.psra_indicators_hexgrid_10km_uc \
            ORDER BY psra_indicators_hexgrid_10km_uc."gridid_10" \
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
        view="opendrr_psra_indicators_hexgrid_25km_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_psra_national.psra_indicators_hexgrid_25km \
            ORDER BY psra_indicators_hexgrid_25km."gridid_25" \
            LIMIT {limit} \
            OFFSET {offset}',
    )
    table.postgis2es()

    # hexgrid 25km unclipped aggregation
    table = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_psra_indicators_hexgrid_25km_uc_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_psra_national.psra_indicators_hexgrid_25km_uc \
            ORDER BY psra_indicators_hexgrid_25km_uc."gridid_25" \
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
        view="opendrr_psra_indicators_hexgrid_50km_uc_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_psra_national.psra_indicators_hexgrid_50km_uc \
            ORDER BY psra_indicators_hexgrid_50km_uc."gridid_50" \
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
        view="opendrr_psra_indicators_hexgrid_100km_uc_{}".format(version),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_psra_national.psra_indicators_hexgrid_100km_uc \
            ORDER BY psra_indicators_hexgrid_100km_uc."gridid_100" \
            LIMIT {limit} \
            OFFSET {offset}',
    )
    table.postgis2es()

    # psra Canada agg loss
    psraTable = utils.PostGISTable(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={"settings": {"number_of_shards": 1, "number_of_replicas": 0}}
        ),
        view="opendrr_psra_canada_agg_loss_{}".format(version),
        sqlquerystring="SELECT * \
                    FROM results_psra_canada.psra_canada_agg_loss \
                    LIMIT {limit} \
                    OFFSET {offset}",
    )
    psraTable.postgis2es()

    # psra Canada expected loss
    psraTable = utils.PostGISTable(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={"settings": {"number_of_shards": 1, "number_of_replicas": 0}}
        ),
        view="opendrr_psra_canada_expected_loss_{}".format(version),
        sqlquerystring="SELECT * \
                    FROM results_psra_canada.psra_canada_expected_loss \
                    LIMIT {limit} \
                    OFFSET {offset}",
    )
    psraTable.postgis2es()

    # psra Canada expected loss - 500 year aggregation
    psraTable = utils.PostGISTable(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={"settings": {"number_of_shards": 1, "number_of_replicas": 0}}
        ),
        view="opendrr_psra_canada_expected_loss_500yr_{}".format(version),
        sqlquerystring="SELECT * \
                    FROM results_psra_canada.psra_canada_expected_loss_500yr \
                    LIMIT {limit} \
                    OFFSET {offset}",
    )
    psraTable.postgis2es()

    # psra Canada src loss
    psraTable = utils.PostGISTable(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={"settings": {"number_of_shards": 1, "number_of_replicas": 0}}
        ),
        view="opendrr_psra_canada_src_loss_{}".format(version),
        sqlquerystring="SELECT * \
                    FROM results_psra_canada.psra_canada_src_loss \
                    LIMIT {limit} \
                    OFFSET {offset}",
    )
    psraTable.postgis2es()

    return


if __name__ == "__main__":
    main()
