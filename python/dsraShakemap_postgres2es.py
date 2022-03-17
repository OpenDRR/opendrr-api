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
import argparse


def main():
    args = parse_args()

    # Create shakemap object and load to ES
    dsraTable = utils.PostGISdataset(
        utils.PostGISConnection(),
        utils.ESConnection(settings={
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
        }),
        view="opendrr_dsra_{eqScenario}_shakemap".format(**{
            'eqScenario': args.eqScenario}).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_shakemap \
            ORDER BY dsra_{eqScenario}_shakemap."SiteID" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(**{'eqScenario': args.eqScenario})
    )
    dsraTable.postgis2es()

    # Create load 1km shakemap hexbin
    dsraTable = utils.PostGISdataset(
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
        view="opendrr_dsra_{eqScenario}_shakemap_hexbin_1km".format(**{
            'eqScenario': args.eqScenario}).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_shakemap_hexbin_1km \
            ORDER BY dsra_{eqScenario}_shakemap_hexbin_1km."gridid_1" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(**{'eqScenario': args.eqScenario})
    )
    dsraTable.postgis2es()

    # Create load 1km shakemap hexbin unclipped
    dsraTable = utils.PostGISdataset(
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
        view="opendrr_dsra_{eqScenario}_shakemap_hexbin_1km_uc".format(**{
            'eqScenario': args.eqScenario}).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_shakemap_hexbin_1km_uc \
            ORDER BY dsra_{eqScenario}_shakemap_hexbin_1km_uc."gridid_1" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(**{'eqScenario': args.eqScenario})
    )
    dsraTable.postgis2es()

    # Create load 5km shakemap hexbin
    dsraTable = utils.PostGISdataset(
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
        view="opendrr_dsra_{eqScenario}_shakemap_hexbin_5km".format(**{
            'eqScenario': args.eqScenario}).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_shakemap_hexbin_5km \
            ORDER BY dsra_{eqScenario}_shakemap_hexbin_5km."gridid_5" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(**{'eqScenario': args.eqScenario})
    )
    dsraTable.postgis2es()

    # Create load 5km shakemap hexbin unlcipped
    dsraTable = utils.PostGISdataset(
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
        view="opendrr_dsra_{eqScenario}_shakemap_hexbin_5km_uc".format(**{
            'eqScenario': args.eqScenario}).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_shakemap_hexbin_5km_uc \
            ORDER BY dsra_{eqScenario}_shakemap_hexbin_5km_uc."gridid_5" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(**{'eqScenario': args.eqScenario})
    )
    dsraTable.postgis2es()

    # Create load 10km shakemap hexbin
    dsraTable = utils.PostGISdataset(
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
        view="opendrr_dsra_{eqScenario}_shakemap_hexbin_10km".format(**{
            'eqScenario': args.eqScenario}).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_shakemap_hexbin_10km \
            ORDER BY dsra_{eqScenario}_shakemap_hexbin_10km."gridid_10" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(**{'eqScenario': args.eqScenario})
    )
    dsraTable.postgis2es()

    # Create load 10km shakemap hexbin unclipped
    dsraTable = utils.PostGISdataset(
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
        view="opendrr_dsra_{eqScenario}_shakemap_hexbin_10km_uc".format(**{
            'eqScenario': args.eqScenario}).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_shakemap_hexbin_10km_uc \
            ORDER BY dsra_{eqScenario}_shakemap_hexbin_10km_uc."gridid_10" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(**{'eqScenario': args.eqScenario})
    )
    dsraTable.postgis2es()

    # Create load 25km shakemap hexbin
    dsraTable = utils.PostGISdataset(
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
        view="opendrr_dsra_{eqScenario}_shakemap_hexbin_25km".format(**{
            'eqScenario': args.eqScenario}).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_shakemap_hexbin_25km \
            ORDER BY dsra_{eqScenario}_shakemap_hexbin_25km."gridid_25" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(**{'eqScenario': args.eqScenario})
    )
    dsraTable.postgis2es()

    # Create load 25km shakemap hexbin unclipped
    dsraTable = utils.PostGISdataset(
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
        view="opendrr_dsra_{eqScenario}_shakemap_hexbin_25km_uc".format(**{
            'eqScenario': args.eqScenario}).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_shakemap_hexbin_25km_uc \
            ORDER BY dsra_{eqScenario}_shakemap_hexbin_25km_uc."gridid_25" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(**{'eqScenario': args.eqScenario})
    )
    dsraTable.postgis2es()

    # Create load 50km shakemap hexbin unclipped
    dsraTable = utils.PostGISdataset(
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
        view="opendrr_dsra_{eqScenario}_shakemap_hexbin_50km_uc".format(**{
            'eqScenario': args.eqScenario}).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_shakemap_hexbin_50km_uc \
            ORDER BY dsra_{eqScenario}_shakemap_hexbin_50km_uc."gridid_50" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(**{'eqScenario': args.eqScenario})
    )
    dsraTable.postgis2es()

    # Create load 100km shakemap hexbin unclipped
    dsraTable = utils.PostGISdataset(
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
        view="opendrr_dsra_{eqScenario}_shakemap_hexbin_100km_uc".format(**{
            'eqScenario': args.eqScenario}).lower(),
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM results_dsra_{eqScenario}.dsra_{eqScenario}_shakemap_hexbin_100km_uc \
            ORDER BY dsra_{eqScenario}_shakemap_hexbin_100km_uc."gridid_100" \
            LIMIT {{limit}} \
            OFFSET {{offset}}'.format(**{'eqScenario': args.eqScenario})
    )
    dsraTable.postgis2es()

    return


def parse_args():
    parser = argparse.ArgumentParser(description="script description")
    parser.add_argument("--eqScenario", type=str, help="Earthquake scenario id", required=True)
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    main()
