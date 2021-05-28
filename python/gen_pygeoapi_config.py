import json
import configparser

from elasticsearch import Elasticsearch
from elasticsearch import helpers

#Main Function
def main():

    auth = get_config_params('config.ini')

    # es = Elasticsearch()
    es = Elasticsearch([auth.get('es', 'es_endpoint')],
                       http_auth=(auth.get('es', 'es_un'),
                                  auth.get('es', 'es_pw')))

    config = """# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2020 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

server:
    bind:
        host: 0.0.0.0
        port: 5000
    url: http://localhost:5000
    mimetype: application/json; charset=UTF-8
    encoding: utf-8
    language: en-US
    # cors: true
    pretty_print: true
    limit: 10
    # templates:
      # path: /path/to/Jinja2/templates
      # static: /path/to/static/folder # css/js/img
    map:
        url: 'http://{s}.tile.osm.org/{z}/{x}/{y}.png'
        attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors, Points &copy 2012 LINZ'
#    manager:
#        name: TinyDB
#        connection: /tmp/pygeoapi-process-manager.db
#        output_dir: /tmp/
    # ogc_schemas_location: /opt/schemas.opengis.net

logging:
    level: ERROR
    # logfile: /tmp/pygeoapi.log

metadata:
    identification:
        title: OpenDRR Web Feature Service
        description: Developer API for geospatial data provided by the OpenDRR platform.
        keywords:
            - land management
            - sustainable development
            - planning
            - natural disasters
        keywords_type: theme
        terms_of_service: http://open.canada.ca/en/open-government-licence-canada/
        url: https://opendrr.github.io/
    license:
        name: Open Government Licence - Canada
        url: http://open.canada.ca/en/open-government-licence-canada/
    provider:
        name: Government of Canada; Natural Resources Canada; Lands and Minerals Sector, Geological Survey of Canada
        url: https://www.nrcan.gc.ca/
    contact:
        name: Hastings, Nicky
        position: Project Manager
        address: 1500 - 605 Robson Street
        city: Vancouver
        stateorprovince: British Columbia
        postalcode: V6B 5J3
        country: Canada
        phone: +01-604-666-0529
        fax: +01-604-666-1124
        email: nicky.hastings@canada.ca
        url: https://www.nrcan.gc.ca/
        hours: Mo-Fr 08:30-16:30
        instructions: During hours of service. Off on weekends.
        role: pointOfContact

resources:\n\n"""

    snippet = """{0}:
        type: collection
        title: {1}
        description: {2}
        keywords:
            - earthquake
        links:
            - type: text/html
              rel: canonical
              title: information
              href: http://www.riskprofiler.ca/
              hreflang: en-US
        extents:
            spatial:
                bbox: [-141.003,41.6755,-52.6174,83.1139]
                crs: http://www.opengis.net/def/crs/OGC/1.3/CRS84
            temporal:
                begin: 2020-08-06
                end: null  # or empty (either means open ended)
        providers:
            - type: feature
              name: Elasticsearch
              data: """ + auth.get('es', 'es_endpoint') + """/{3}
              id_field: {4}"""

    text_file = open("../pygeoapi/opendrr.config.yml", "w")

    id_field = "AssetID"

    indices = es.indices.get('*')
    indices = sorted(indices)

    for index in indices:
        if (index[0] == '.'):
            continue

        if index[-2:] == "_s":
            id_field = "Sauid" 

        config += "    " +snippet.format(index, index, index, index, id_field) + "\n\n"

    text_file.write(config)
    text_file.close()

def get_config_params(args):
    """
    Parse Input/Output columns from supplied *.ini file
    """
    configParseObj = configparser.ConfigParser()
    configParseObj.read(args)
    return configParseObj



if __name__ == '__main__':
    main() 