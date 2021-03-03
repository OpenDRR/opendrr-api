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

    config = ""

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
            bbox: [-180,-90,180,90]
            crs: http://www.opengis.net/def/crs/OGC/1.3/CRS84
        temporal:
            begin: 2020-08-06
            end: null  # or empty (either means open ended)
    providers:
        - type: feature
          name: Elasticsearch
          data: """ + auth.get('es', 'es_endpoint') + """/{3}
          id_field: {4}"""

    text_file = open("pygeoapi_config.txt", "w")

    id_field = "AssetID"

    for index in es.indices.get('*'):
        if (index[0] == '.'):
            continue

        if index[-2:] == "_s":
            id_field = "Sauid" 

        config += snippet.format(index, index, index, index, id_field) + "\n\n"

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