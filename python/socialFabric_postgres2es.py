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
Script to convert Social Fabric Views to ElasticSearch Index
Can be run from the command line with mandatory arguments
Run this script with a command like:
python3 socialFabric_postgres2es.py
    --type="indicators"
    --aggregation="sauid"
    --geometry=geom_poly
    --idField="Sauid"
"""


# Main Function
def main():
    args = parse_args()

    config = utils.get_config_params("config.ini")
    version = config.get("es", "version")

    if args.aggregation.lower() == "sauid":
        aggregation = args.aggregation[0].lower()
    else:
        aggregation = args.aggregation

    # index settings
    if args.geometry == "geom_poly" or "geom":
        table = utils.PostGISdataset(
            utils.PostGISConnection(),
            utils.ESConnection(
                settings={
                    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                    "mappings": {"properties": {"geometry": {"type": "geo_shape"}}},
                }
            ),
            view="opendrr_nhsl_social_fabric_indicators_{agg}_{version}".format(
                **{"agg": aggregation, "version": version}
            ),
            sqlquerystring='SELECT *, ST_AsGeoJSON({geom}) \
                FROM \
                results_nhsl_social_fabric.nhsl_social_fabric_indicators_{agg} \
                ORDER BY "{sort_field}" \
                LIMIT {{limit}} \
                OFFSET {{offset}}'.format(
                **{
                    "geom": args.geometry,
                    "agg": aggregation,
                    "sort_field": args.sortfield,
                }
            ),
        )

    elif args.geometry == "geom_point":
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
            view="opendrr_nhsl_social_fabric_indicators_{agg}_{version}".format(
                **{"agg": args.aggregation[0].lower(), "version": version}
            ),
            sqlquerystring='SELECT *, ST_AsGeoJSON(geom_point) \
                FROM \
                results_nhsl_social_fabric.nhsl_social_fabric_indicators_{agg} \
                ORDER BY "{sort_field}" \
                LIMIT {{limit}} \
                OFFSET {{offset}}'.format(
                **{"agg": aggregation, "sort_field": args.sortfield}
            ),
        )

    table.postgis2es()

    return


def parse_args():
    parser = argparse.ArgumentParser(description="load exposure PostGIS to ES")
    parser.add_argument(
        "--aggregation", type=str, help="sauid or hexgrid_xxkm", required=True
    )
    parser.add_argument(
        "--geometry", type=str, help="geom_point or geom_poly", required=True
    )
    parser.add_argument(
        "--sortfield",
        type=str,
        help="Sauid or gridid_100, gridid_25 etc.",
        required=True,
    )
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    main()
