#!/usr/bin/python3
# =================================================================
# SPDX-License-Identifier: MIT
#
# Copyright (C) 2020-2021 Government of Canada
#
# Main Authors: Drew Rotheram <drew.rotheram-clarke@canada.ca>
#
# =================================================================

import configparser

from elasticsearch import Elasticsearch


def get_config_params(args):
    """
    Parse Input/Output columns from supplied *.ini file
    """
    configParseObj = configparser.ConfigParser()
    configParseObj.read(args)
    return configParseObj


auth = get_config_params("config.ini")

es = Elasticsearch(
    [auth.get("es", "es_endpoint")],
    http_auth=(auth.get("es", "es_un"), auth.get("es", "es_pw")),
)

indexList = es.cat.indices(index="*_v1.4.0", h="index", s="index:desc").split()

for index in indexList:
    # print(index)
    # print(index.rsplit("_", 1)[0])
    indexBaseName = index.rsplit("_", 1)[0] + "test_alias"
    # print(index.split("_")[0:-1].join())
    es.indices.put_alias(index=index, name=indexBaseName)
