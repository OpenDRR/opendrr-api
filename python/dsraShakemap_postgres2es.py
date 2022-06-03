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
    args = parse_args()

    config = utils.get_config_params("config.ini")
    version = config.get("es", "version")

    # Create shakemap object and load to ES
    dsraTable = utils.PostGISdataset(
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
        view="opendrr_dsra_{eqScenario}_shakemap_{version}".format(
            **{"eqScenario": args.eqScenario, "version": version}
        ).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_shakemap \
            ORDER BY dsra_{eqScenario}_shakemap."SiteID" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(
            **{"eqScenario": args.eqScenario}
        ),
    )
    dsraTable.postgis2es()

    # Create load 1km shakemap hexgrid
    dsraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_dsra_{eqScenario}_shakemap_hexgrid_1km_{version}".format(
            **{"eqScenario": args.eqScenario, "version": version}
        ).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_sm_hg_1 \
            ORDER BY dsra_{eqScenario}_sm_hg_1."gridid_1" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(
            **{"eqScenario": args.eqScenario}
        ),
    )
    dsraTable.postgis2es()

    # Create load 1km shakemap hexgrid unclipped
    dsraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_dsra_{eqScenario}_shakemap_hexgrid_1km_uc_{version}".format(
            **{"eqScenario": args.eqScenario, "version": version}
        ).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_sm_hg_1_uc \
            ORDER BY dsra_{eqScenario}_sm_hg_1_uc."gridid_1" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(
            **{"eqScenario": args.eqScenario}
        ),
    )
    dsraTable.postgis2es()

    # Create load 5km shakemap hexgrid
    dsraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_dsra_{eqScenario}_shakemap_hexgrid_5km_{version}".format(
            **{"eqScenario": args.eqScenario, "version": version}
        ).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_sm_hg_5 \
            ORDER BY dsra_{eqScenario}_sm_hg_5."gridid_5" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(
            **{"eqScenario": args.eqScenario}
        ),
    )
    dsraTable.postgis2es()

    # Create load 5km shakemap hexgrid unclipped
    dsraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_dsra_{eqScenario}_shakemap_hexgrid_5km_uc_{version}".format(
            **{"eqScenario": args.eqScenario, "version": version}
        ).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_sm_hg_5_uc \
            ORDER BY dsra_{eqScenario}_sm_hg_5_uc."gridid_5" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(
            **{"eqScenario": args.eqScenario}
        ),
    )
    dsraTable.postgis2es()

    # Create load 10km shakemap hexgrid
    dsraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_dsra_{eqScenario}_shakemap_hexgrid_10km_{version}".format(
            **{"eqScenario": args.eqScenario, "version": version}
        ).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_sm_hg_10 \
            ORDER BY dsra_{eqScenario}_sm_hg_10."gridid_10" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(
            **{"eqScenario": args.eqScenario}
        ),
    )
    dsraTable.postgis2es()

    # Create load 10km shakemap hexgrid unclipped
    dsraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_dsra_{eqScenario}_shakemap_hexgrid_10km_uc_{version}".format(
            **{"eqScenario": args.eqScenario, "version": version}
        ).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_sm_hg_10_uc \
            ORDER BY dsra_{eqScenario}_sm_hg_10_uc."gridid_10" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(
            **{"eqScenario": args.eqScenario}
        ),
    )
    dsraTable.postgis2es()

    # Create load 25km shakemap hexgrid
    dsraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_dsra_{eqScenario}_shakemap_hexgrid_25km_{version}".format(
            **{"eqScenario": args.eqScenario, "version": version}
        ).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_sm_hg_25 \
            ORDER BY dsra_{eqScenario}_sm_hg_25."gridid_25" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(
            **{"eqScenario": args.eqScenario}
        ),
    )
    dsraTable.postgis2es()

    # Create load 25km shakemap hexgrid unclipped
    dsraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_dsra_{eqScenario}_shakemap_hexgrid_25km_uc_{version}".format(
            **{"eqScenario": args.eqScenario, "version": version}
        ).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_sm_hg_25_uc \
            ORDER BY dsra_{eqScenario}_sm_hg_25_uc."gridid_25" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(
            **{"eqScenario": args.eqScenario}
        ),
    )
    dsraTable.postgis2es()

    # Create load 50km shakemap hexgrid unclipped
    dsraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_dsra_{eqScenario}_shakemap_hexgrid_50km_uc_{version}".format(
            **{"eqScenario": args.eqScenario, "version": version}
        ).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_sm_hg_50_uc \
            ORDER BY dsra_{eqScenario}_sm_hg_50_uc."gridid_50" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(
            **{"eqScenario": args.eqScenario}
        ),
    )
    dsraTable.postgis2es()

    # Create load 100km shakemap hexgrid unclipped
    dsraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(
            settings={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
            }
        ),
        view="opendrr_dsra_{eqScenario}_shakemap_hexgrid_100km_uc_{version}".format(
            **{"eqScenario": args.eqScenario, "version": version}
        ).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_sm_hg_100_uc \
            ORDER BY dsra_{eqScenario}_sm_hg_100_uc."gridid_100" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(
            **{"eqScenario": args.eqScenario}
        ),
    )
    dsraTable.postgis2es()

    return


def parse_args():
    parser = argparse.ArgumentParser(description="script description")
    parser.add_argument(
        "--eqScenario", type=str, help="Earthquake scenario id", required=True
    )
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    main()
