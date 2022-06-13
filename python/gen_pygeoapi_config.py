#!/usr/bin/python3

"""
Generate configuration files for OpenDRR pygeoapi (OGC API - Features)
"""

import configparser

import yaml
from elasticsearch import Elasticsearch


def main():
    """
    Main function
    """

    auth = get_config_params("config.ini")

    # es = Elasticsearch()
    # es = Elasticsearch([auth.get("es", "es_endpoint")],
    #                    http_auth=(auth.get("es", "es_un"),
    #                               auth.get("es", "es_pw")))

    elasticsearch = Elasticsearch([auth.get("es", "es_endpoint")])

    text_file = open("../pygeoapi/opendrr.config.yml", "w", encoding="utf-8")

    with open("../opendrr_config_template.yml", "r", encoding="utf-8") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    version = auth.get("es", "index_version")

    lyrs = config["resources"]

    indices = elasticsearch.indices.get(index="opendrr_*")

    print("\nProcessing opendrr_config_template.yml...")
    for k in list(lyrs.keys()):
        k_version = k + "_" + version
        if k_version not in indices:
            print("REMOVING TEMPLATE ENTRY FOR: " + k_version)
            del lyrs[k_version]
        else:
            # write in the ES endpoint configured in the config.ini
            new = lyrs[k]["providers"][0]["data"].replace(
                "ES_ENDPOINT", auth.get("es", "es_endpoint")
            )
            new = new.replace("INDEX_VERSION", version)

            lyrs[k]["providers"][0]["data"] = new

    print("\nDone!")

    # list indices missing from opendrr_config_template.yml
    print("\nListing indices missing from opendrr_config_template.yml...")
    for i in list(indices.keys()):
        if version not in i:
            continue
        if i.replace("_" + version, "") not in lyrs:
            print("MISSING IN CONFIGURATION TEMPLATE: " + i)
    print("\nDone!")

    # write the layers to the configuration resources element
    config["resources"] = lyrs

    # save the configuration
    print("\nSaving configuration file to disk...")
    text_file.write(yaml.dump(config))
    text_file.close()
    print("\nDone!\n")


def get_config_params(args):
    """
    Parse Input/Output columns from supplied *.ini file
    """

    config_parse_obj = configparser.ConfigParser()
    config_parse_obj.read(args)
    return config_parse_obj


if __name__ == "__main__":
    main()
