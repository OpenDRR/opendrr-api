# opendrr-api
[
![GitHub Super-Linter](https://github.com/OpenDRR/opendrr-api/workflows/Lint%20Code%20Base/badge.svg)](https://github.com/marketplace/actions/super-linter)
![GitHub](https://img.shields.io/github/license/OpenDRR/opendrr-api)

API REST pour les données OpenDRR
<img src="https://github.com/OpenDRR/documentation/blob/master/models/opendrr-stack.png" width="600">

opendrr-api est un référentiel utilisé conjointement avec le référentiel [model-factory](https://github.com/opendrr/model-factory/). Il contient des scripts python, shell et dockerfile pour mettre en œuvre avec succès une API REST pour les données openDRR qui comprennent une base de données PostGIS, Elasticsearch, Kibana et pygeoapi. Le référentiel model-factory contient les scripts nécessaires pour transformer les données sources opendrr en indicateurs de profil de risque qui vont dans la base de données PostGIS.

 - **postgis/**
	 - Scripts pour configurer la base de données PostGIS.
- **pygeoapi/**
	- Scripts pour configurer pygeoapi.
- **python/**
	- Scripts pour traiter les données sources d'opendrr, les scripts de model-factory, et charger les index d'elasticsearch.
- docker-compose-run.yml, docker-compose.yml, opendrr-config_template.yml
    - fichiers de configuration de docker-compose et d'opendrr
- requirements.txt
	- liste des modules et des versions qui doivent être installés.  `$ pip install -r requirements.txt`

Référez-vous à la section [releases](https://github.com/OpenDRR/opendrr-api/releases) pour les derniers changements de version.

### Comment construire votre propre pile - Configuration dans votre environnement local

### 1. Prérequis

- [Docker engine](https://docs.docker.com/get-docker/) installé et en cours d'exécution
- Téléchargez ou clonez ce dépôt dans votre environnement de développement local.

### 2. Modifier les paramètres de l'environnement Docker

Faites une copie du fichier `sample.env` et renommez-le en `.env`. Apportez des modifications si nécessaire, sinon laissez les paramètres par défaut.

Les paramètres ci-dessous se trouvent dans le fichier .env et peuvent être ajustés à **'true'** ou **'false'** selon vos préférences. Par exemple, si vous souhaitez charger les données PSRA dans leur propre base de données PostGIS, Elasticsearch et Kibana, vous pouvez définir processPSRA et loadPsraModels sur 'true' et toutes les autres options sur 'false'. Le fait de spécifier les fonctionnalités qui sont uniquement nécessaires peut vous faire gagner du temps.

Traitement des données sources Scénarios sismiques (DSRA) / Risque sismique probabiliste (PSRA) :

    processDSRA=true
    processPSRA=true

Chargement des index dans Elasticsearch et Kibana :

    loadDsraScenario=true
    loadPsraModels=true
    loadHazardThreat=false (menace de danger)
    loadPhysicalExposure=true
    loadRiskDynamics=true
    loadSocialFabric=true

### 3. Modifiez la configuration du conteneur Python

Faites une copie de `python/sample_config.ini` et renommez-la `config.ini`. Ouvrez ce fichier dans un éditeur, ajoutez le [github_token](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token) requis et définissez les paramètres restants comme suit :

    [auth]
    # Github Token for Private Repo Accesss
    github_token = a00049ba79152d03380c34652f2cb612

    [rds]
    # PostGIS Connection Details
    postgres_host = db-opendrr
    postgres_port = 5432
    postgres_un = postgres
    postgres_pw = password
    postgres_db = opendrr
    postgres_address = db-opendrr:5432/opendrr

    [es]
    # Elasticsearch Connection Details
    es_un = elastic
    es_pw = changeme
    es_endpoint = elasticsearch-opendrr:9200
    kibana_endpoint = localhost:5601
    # version of indices to be configured (i.e. v1.4.1)
    index_version = v1.4.4
    
### 4. Exécutez docker-compose

    docker-compose up --build

> NOTE : vous verrez des erreurs lancées par le conteneur opendrr-api_pygeoapi-opendrr_1 pendant la construction de la pile. Celles-ci peuvent être ignorées.

Une fois la pile construite, l'utilisateur devra vérifier que tout fonctionne.

> NOTE : vous pouvez arrêter la pile quand vous le souhaitez avec `Ctrl-C` ou `docker-compose stop`. Voir ci-dessous comment vous pouvez remettre la pile en marche sans la recompiler.

### 5. Vérifiez que tout fonctionne

Vérifiez Elasticsearch pour vous assurer que les index ont été créés :

<http://localhost:9200/_cat/indices?v&pretty>

Vous devriez voir quelque chose de similaire à :

    health status index ...
    green  open   afm7p2_lrdmf_scenario_shakemap_intensity_building
    green  open   .apm-custom-link
    green  open   afm7p2_lrdmf_damage_state_building
    green  open   .kibana_task_manager_1
    green  open   afm7p2_lrdmf_social_disruption_building
    green  open   .apm-agent-configuration
    green  open   afm7p2_lrdmf_recovery_time_building
    green  open   afm7p2_lrdmf_scenario_shakemap_intensity_building
    green  open   afm7p2_lrdmf_casualties_building
    green  open   .kibana_1

Vous pouvez explorer les index dans Elasticsearch à l'aide de Kibana.

<http://localhost:5601>

Vérifiez pygeoapi pour vous assurer que les collections sont accessibles.

<http://localhost:5001/collections>

Les collections de fonctionnalités sont accessibles comme suit ou en cliquant sur les liens fournis sur la page des collections.

<http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building/items?f=json&limit=1>

Vous devriez voir quelque chose de similaire à :

    {
        "type": "FeatureCollection",
        "features": [
            {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [
                -117.58484079,
                49.58943143
                ]
            },
            "properties": {
                "AssetID": "59002092-RES3A-W1-PC",
                "Sauid": "59002092",
                "sL_Asset_b0": 27.602575,
                "sL_Bldg_b0": 27.602575,
                "sLr_Bldg_b0": 1,
                "sLr2_BCR_b0": 0.000819,
                "SLr2_RoI": 0.001359,
                "sL_Str_b0": 27.602575,
                "sLsd_Str_b0": 23.4638,
                "sL_NStr_b0": 0,
                "sLsd_NStr_b0": 0,
                "sL_Cont_b0": 0,
                "sLsd_Cont_b0": 0,
                "sL_Asset_r2": 0.311704,
                "sL_Bldg_r2": 0.311704,
                "sLr_Bldg_r2": 1,
                "sL_Str_r2": 0.311704,
                "sLsd_Str_r2": 0.264966,
                "sL_NStr_r2": 0,
                "sLsd_NStr_r2": 0,
                "sL_Cont_r2": 0,
                "sLsd_Cont_r2": 0,
                "geom_point": "0101000020E6100000AD9A10086E655DC0D18A357D72CB4840"
            },
            "id": "59002092"
            }
        ],
        "numberMatched": 173630,
        "numberReturned": 1,
        "links": [
            {
            "type": "application/geo+json",
            "rel": "self",
            "title": "This document as GeoJSON",
            "href": "http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building/items?f=json&amp;limit=1"
            },
            {
            "rel": "alternate",
            "type": "application/ld+json",
            "title": "This document as RDF (JSON-LD)",
            "href": "http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building/items?f=jsonld&amp;limit=1"
            },
            {
            "type": "text/html",
            "rel": "alternate",
            "title": "This document as HTML",
            "href": "http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building/items?f=html&amp;limit=1"
            },
            {
            "type": "application/geo+json",
            "rel": "next",
            "title": "items (next)",
            "href": "http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building/items?startindex=1&amp;limit=1"
            },
            {
            "type": "application/json",
            "title": "Economic loss buildings",
            "rel": "collection",
            "href": "http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building"
            }
        ],
        "timeStamp": "2020-08-18T22:46:10.513010Z"
        }

### Interagir avec les points de terminaison

### Interroger pygeoapi

Reportez-vous à la documentation de pygeoapi pour des conseils généraux :

<!-- textlint-disable -->
<http://localhost:5001/openapi?f=html>
<!-- textlint-enable -->`

> NOTE : l'interrogation est actuellement limitée à l'étendue spatiale et aux requêtes de valeur exacte. Pour des requêtes plus complexes, utilisez Elasticsearch (voir ci-dessous).

#### Pour filtrer sur un attribut spécifique

<http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building/items?sH_Mag=7.2>

#### Pour filtrer en utilisant une boîte de délimitation

<http://localhost:5001/collections/afm7p2_lrdmf_scenario_shakemap_intensity_building/items?bbox=-119,48.8,-118.9,49.8&f=json>

### Interrogation d'Elasticsearch

#### Requête de plage

<http://localhost:9200/afm7p2_lrdmf_scenario_shakemap_intensity_building/_search?q=properties.sH_PGA :[0.047580+À+0.047584]>

OU en utilisant curl :

    curl -XGET "http://localhost:9200/afm7p2_lrdmf_scenario_shakemap_intensity_building/_search" -H 'Content-Type:
    application/json' -d'
    {
        "query": {
            "range": {
                "properties.sH_PGA": {
                    "gte": 0.047580,
                    "lte": 0.047584
                }
            }
        }
    }'

#### Valeur spécifique

<http://localhost:9200/afm7p2_lrdmf_scenario_shakemap_intensity_building/_search?q=properties.sH_PGA:0.047584>

OU en utilisant curl :

    curl -XGET "http://localhost:9200/afm7p2_lrdmf_scenario_shakemap_intensity_building/_search" -H 'Content-Type:
    application/json' -d'
    {
        "query": {
            "match": {
                "properties.sH_PGA" : 0.047584
            }
        }
    }'

#### Requête de boîte englobante

    curl -XGET "http://localhost:9200/afm7p2_lrdmf_scenario_shakemap_intensity_building/_search" -H 'Content-Type:
    application/json' -d'
    {
        "query": {
            "bool": {
                "filter": [
                    {
                        "geo_shape": {
                            "geometry": {
                                "shape": {
                                    "type": "envelope",
                                    "coordinates": [ [ -118.7, 50 ], [ -118.4, 49.9 ] ]
                                },
                                "relation": "intersects"
                            }
                        }
                    }
                ]
            }
        }
    }'

#### Requête la plus proche

    curl -XGET "http://localhost:9200/nhsl_hazard_threat_all_indicators_s/_search" -H 'Content-Type:
    application/json' -d'
    {
      "query": {
        "geo_shape": {
          "geometry": {
            "shape": {
              "type": "circle",
              "radius": "20km",
              "coordinates": [ -118, 49 ]
            }
          }
        }
      }
    }'

## Interaction avec la base de données spatiale

La base de données spatiale est implémentée à l'aide de PostGIS. Vous pouvez vous connecter à PostGIS en utilisant [pgAdmin](https://www.pgadmin.org/) avec les paramètres de connexion dans votre fichier `.env`. Par exemple :

    POSTGRES_USER: postgres
    POSTGRES_PASSWORD: password
    POSTGRES_PORT: 5432
    DB_NAME: opendrr

### Ajout de jeux de données à QGIS

Vous avez deux options :

#### Se connecter à PostGIS

1. Ajoutez une "Nouvelle connexion" en faisant un clic droit sur le type de données "PostGIS" dans le navigateur.
2. Saisissez un nom pour votre connexion (par exemple, "OpenDRR").
3. Ajoutez les informations d'identification selon votre fichier `.env` (voir ci-dessus).
4. Cliquez sur le bouton "OK".

#### Connexion à OGC OpenAPI - Caractéristiques

1. Ajoutez une "Nouvelle connexion" en faisant un clic droit sur le type de données "WFS / OGC API -Features" dans le navigateur.
2. Saisissez un nom pour votre connexion (par exemple, "OpenDRR").
3. Entrez `http://localhost:5001` dans le champ URL.
4. Sélectionnez "OGC API - Features" dans la liste déroulante "Version".
4. Cliquez sur le bouton "OK".

## Démarrer/arrêter la pile

Une fois que la pile est construite, vous n'avez besoin de la recompiler que lorsqu'il y a de nouvelles données. Le script `docker-compose-run.yml` est une surcharge que vous pouvez utiliser pour exécuter la pile construite - il ne crée pas le conteneur python qui tire le dernier code et les données de GitHub pour alimenter la pile.

Pour démarrer la pile :

    docker-compose -f docker-compose-run.yml start

Pour arrêter la pile :

    docker-compose -f docker-compose-run.yml stop

## Mise à jour ou reconstruction de la pile

Mettez la pile hors service et supprimez les volumes :

    docker-compose down -v

Reconstruisez la pile :

    docker-compose up --build