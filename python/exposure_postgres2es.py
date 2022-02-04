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


'''
Script to convert Physical Exposure Views to ElasticSearch Index
Can be run from the command line with mandatory arguments
Run this script with a command like:
python3 exposure_postgres2es.py
    --type="assets"
    --aggregation="building"
    --geometry=geom_point
    --idField="BldgID"
'''


# Main Function
def main():
    args = parse_args()

    # sauid level aggregation
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
        view="opendrr_nhsl_physical_exposure_indicators_s",
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom_poly) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_s \
            ORDER BY nhsl_physical_exposure_indicators_s."Sauid" \
            LIMIT {limit} \
            OFFSET {offset}'
    )
    table.postgis2es()

    # building level aggregation
    table = utils.PostGISdataset(
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
        view="opendrr_nhsl_physical_exposure_indicators_b",
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom_point) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_b \
            ORDER BY nhsl_physical_exposure_indicators_b."BldgID" \
            LIMIT {limit} \
            OFFSET {offset}'
    )
    table.postgis2es()

    # hexbin 5km aggregation
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
        view="opendrr_nhsl_physical_exposure_indicators_hexbin_5km",
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexbin_5km \
            ORDER BY nhsl_physical_exposure_indicators_hexbin_5km."gridid_5" \
            LIMIT {limit} \
            OFFSET {offset}'
    )
    table.postgis2es()

    # hexbin 10km aggregation
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
        view="opendrr_nhsl_physical_exposure_indicators_hexbin_10km",
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexbin_10km \
            ORDER BY nhsl_physical_exposure_indicators_hexbin_10km."gridid_10" \
            LIMIT {limit} \
            OFFSET {offset}'
    )
    table.postgis2es()

    # hexbin 25km aggregation
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
        view="opendrr_nhsl_physical_exposure_indicators_hexbin_25km",
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexbin_25km \
            ORDER BY nhsl_physical_exposure_indicators_hexbin_25km."gridid_25" \
            LIMIT {limit} \
            OFFSET {offset}'
    )
    table.postgis2es()

    # hexbin 50km aggregation
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
        view="opendrr_nhsl_physical_exposure_indicators_hexbin_50km",
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexbin_50km \
            ORDER BY nhsl_physical_exposure_indicators_hexbin_50km."gridid_50" \
            LIMIT {limit} \
            OFFSET {offset}'
    )
    table.postgis2es()

    # hexbin 100km aggregation
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
        view="opendrr_nhsl_physical_exposure_indicators_hexbin_100km",
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexbin_100km \
            ORDER BY nhsl_physical_exposure_indicators_hexbin_100km."gridid_100" \
            LIMIT {limit} \
            OFFSET {offset}'
    )
    table.postgis2es()

    # hexbin global fabric
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
        view="opendrr_nhsl_physical_exposure_indicators_hexbin_global_fabric",
        sqlquerystring='SELECT *, ST_AsGeoJSON(geom) \
            FROM \
            results_nhsl_physical_exposure.nhsl_physical_exposure_indicators_hexbin_global_fabric \
            ORDER BY nhsl_physical_exposure_indicators_hexbin_global_fabric."gridid" \
            LIMIT {limit} \
            OFFSET {offset}'
    )
    table.postgis2es()

    return


def parse_args():
    parser = argparse.ArgumentParser(description="load exposure PostGIS to ES")
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    main()
