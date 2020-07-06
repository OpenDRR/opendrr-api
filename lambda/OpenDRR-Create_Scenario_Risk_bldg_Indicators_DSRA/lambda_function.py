
import configparser
import os
import sys
import psycopg2



def lambda_handler(event, context):
    auth = get_config_params('config.ini')
    eqScenario = 'afm_7p3_gsm' #os.environ['EQSCENARIO']
    #retrofitPrefix = os.environ['RETROFITPREFIX']
    realization = 'rlz_1' #os.environ['REALIZATION']

    connection = psycopg2.connect(user = auth.get('rds', 'postgres_un'),
                                    password = auth.get('rds', 'postgres_pw'),
                                    host = auth.get('rds', 'postgres_host'),
                                    port = auth.get('rds', 'postgres_port'),
                                    database = auth.get('rds', 'postgres_db'))

    sqlquerystring = open(r'Create_scenario_risk_building_indicators_ALL.psql', 'r').read().format(**{'eqScenario':eqScenario, 
                                                                                                        'realization':realization})

    cursor = connection.cursor()
    cursor.execute(sqlquerystring)
    cursor.commit()
    cursor.close()
    return print(True)
    
def get_config_params(args):
    """
    Parse config params from .ini file
    """
    configParseObj = configparser.ConfigParser()
    configParseObj.read(args)
    return configParseObj
    
    
    