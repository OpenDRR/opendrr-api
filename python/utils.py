#!/usr/bin/python3
# =================================================================
# SPDX-License-Identifier: MIT
#
# Copyright (C) 2020-2021 Government of Canada
#
# Main Authors: Drew Rotheram <drew.rotheram-clarke@canada.ca>
#               Joost van Ulden <joost.vanulden@canada.ca>
# =================================================================

import configparser
import decimal
import json

import psycopg2
from elasticsearch import Elasticsearch, helpers


class ESConnection:
    def __init__(self, settings):
        self._settings = settings
        self._auth = get_config_params("config.ini")

    def settings(self):
        return self._settings

    pass


class PostGISConnection:
    def __init__(self):
        self._auth = get_config_params("config.ini")
        self._pgConnection = psycopg2.connect(
            user=self._auth.get("rds", "postgres_un"),
            password=self._auth.get("rds", "postgres_pw"),
            host=self._auth.get("rds", "postgres_host"),
            port=self._auth.get("rds", "postgres_port"),
            database=self._auth.get("rds", "postgres_db"),
        )

    def auth(self):
        return self._auth

    def pgConnection(self):
        return self._pgConnection

    pass


class PostGISdataset:
    """A class to represent a dataset stored
    in PostGIS with methods to connect to
    a PostGIS database, query the table into
    a geojson object and post that geojson
    to an ElasticSearch instance
    """

    LIMIT = 10000
    OFFSET = 0

    def __init__(self, PostGISConnection, ESConnection, view, sqlquerystring):
        self._pgConnection = PostGISConnection
        self._esConnection = ESConnection
        self._view = view
        self._sqlquerystring = sqlquerystring
        self._auth = get_config_params("config.ini")

    def pgConnection(self):
        return self._pgConnection

    def esConnection(self):
        return self._esConnection

    def view(self):
        return self._view

    def auth(self):
        return self._auth

    def sqlquerystring(self):
        return self._sqlquerystring

    def getGeoJson(self, sqlquerystring, pgConnection):
        cur = pgConnection.pgConnection().cursor()
        cur.execute(sqlquerystring)
        rows = cur.fetchall()
        if rows:
            columns = [name[0] for name in cur.description]
            geomIndex = columns.index("st_asgeojson")
            feature_collection = {"type": "FeatureCollection", "features": []}

            # Format table into a geojson format for ES/Kibana consumption
            for row in rows:
                feature = {
                    "type": "Feature",
                    "geometry": json.loads(row[geomIndex]),
                    "properties": {},
                }
                for index, column in enumerate(columns):
                    if column != "st_asgeojson":
                        value = row[index]
                        feature["properties"][column] = value
                feature_collection["features"].append(feature)
            geojsonobject = json.dumps(
                feature_collection, indent=2, default=decimal_default
            )
            return geojsonobject
        else:
            return None

    def initializeElasticSearchIndex(self, esConnection, auth, view):
        es = Elasticsearch(
            [auth.get("es", "es_endpoint")],
            http_auth=(auth.get("es", "es_un"), auth.get("es", "es_pw")),
        )
        if es.indices.exists(view):
            es.indices.delete(view)
        settings = esConnection.settings()
        es.indices.create(index=view, body=settings, request_timeout=90)
        return

    def populateElasticSearchIndex(self, esConnection, geojsonobject, auth, view):
        d = json.loads(geojsonobject)
        es = Elasticsearch(
            [auth.get("es", "es_endpoint")],
            http_auth=(auth.get("es", "es_un"), auth.get("es", "es_pw")),
            timeout=30,
            max_retries=10,
            retry_on_timeout=True,
        )
        helpers.bulk(es, gendata(d, view), raise_on_error=False)
        return

    def postgis2es(self):
        self.initializeElasticSearchIndex(self.esConnection(), self.auth(), self.view())
        sqlquerystring = self.sqlquerystring().format(
            **{"limit": self.LIMIT, "offset": self.OFFSET}
        )
        geojsonobject = self.getGeoJson(sqlquerystring, self.pgConnection())
        while geojsonobject is not None:

            print(sqlquerystring)
            self.populateElasticSearchIndex(
                self.esConnection(), geojsonobject, self.auth(), self.view()
            )
            self.OFFSET += self.LIMIT

            sqlquerystring = self.sqlquerystring().format(
                **{"limit": self.LIMIT, "offset": self.OFFSET}
            )
            geojsonobject = self.getGeoJson(sqlquerystring, self.pgConnection())

        return


class PostGISPointDataset(PostGISdataset):
    def getGeoJson(self, sqlquerystring, pgConnection):
        cur = pgConnection.pgConnection().cursor()
        cur.execute(sqlquerystring)
        rows = cur.fetchall()
        if rows:
            columns = [name[0] for name in cur.description]
            geomIndex = columns.index("st_asgeojson")
            feature_collection = {"type": "FeatureCollection", "features": []}

            # Format table into a geojson format for ES/Kibana consumption
            for row in rows:
                coordinates = json.loads(row[geomIndex])["coordinates"]
                feature = {
                    "type": "Feature",
                    "geometry": json.loads(row[geomIndex]),
                    "coordinates": coordinates,
                    "properties": {},
                }
                for index, column in enumerate(columns):
                    if column != "st_asgeojson":
                        value = row[index]
                        feature["properties"][column] = value
                feature_collection["features"].append(feature)
            geojsonobject = json.dumps(
                feature_collection, indent=2, default=decimal_default
            )
            return geojsonobject
        else:
            return None


class PostGISTable(PostGISdataset):
    def getGeoJson(self, sqlquerystring, pgConnection):
        cur = pgConnection.pgConnection().cursor()
        cur.execute(sqlquerystring)
        rows = cur.fetchall()
        if rows:
            columns = [name[0] for name in cur.description]
            # geomIndex = columns.index('st_asgeojson')
            feature_collection = {"type": "FeatureCollection", "features": []}

            # Format table into a geojson format for ES/Kibana consumption
            for row in rows:
                # coordinates = json.loads(row[geomIndex])['coordinates']
                feature = {
                    "type": "Feature",
                    # 'geometry': json.loads(row[geomIndex]),
                    # 'coordinates': coordinates,
                    "properties": {},
                }
                for index, column in enumerate(columns):
                    if column != "st_asgeojson":
                        value = row[index]
                        feature["properties"][column] = value
                feature_collection["features"].append(feature)
            geojsonobject = json.dumps(
                feature_collection, indent=2, default=decimal_default
            )
            return geojsonobject
        else:
            return None


def gendata(data, view):
    for item in data["features"]:
        yield {"_index": view, "_source": item}


# Function to handle decimal encoder error
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


def get_config_params(args):
    """
    Parse Input/Output columns from supplied *.ini file
    """
    configParseObj = configparser.ConfigParser()
    configParseObj.read(args)
    return configParseObj
