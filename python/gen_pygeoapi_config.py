#!/usr/bin/python3

import configparser
import yaml

from elasticsearch import Elasticsearch


# Main Function
def main():

    auth = get_config_params('config.ini')

    # es = Elasticsearch()
    # es = Elasticsearch([auth.get('es', 'es_endpoint')],
    #                    http_auth=(auth.get('es', 'es_un'),
    #                               auth.get('es', 'es_pw')))

    es = Elasticsearch([auth.get('es', 'es_endpoint')])

    text_file = open("../pygeoapi/opendrr.config.yml", "w")

    with open('../opendrr_config_template.yml', 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    lyrs = config['resources']

    indices = es.indices.get('*')

    for k in list(lyrs.keys()):
        if k not in indices:
            del lyrs[k]
        else:
            # write in the ES endpoint configured in the config.ini
            new = lyrs[k]['providers'][0]['data'].replace(
                'ES_ENDPOINT',
                auth.get(
                    'es',
                    'es_endpoint'
                    )
                )
            lyrs[k]['providers'][0]['data'] = new

    # list indices missing from opendrr_config_template.yml
    for i in list(indices.keys()):
        if i not in lyrs:
            print('MISSING IN CONFIGURATION TEMPLATE: ' + i)

    config['resources'] = lyrs

    text_file.write(yaml.dump(config))
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
