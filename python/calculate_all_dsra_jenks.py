# =================================================================
#
# Authors: Drew Rotheram <drew.rotheram@gmail.com>
#
# =================================================================


import os
import configparser

from elasticsearch import Elasticsearch

def get_config_params(args):
    """
    Parse Input/Output columns from supplied *.ini file
    """
    configParseObj = configparser.ConfigParser()
    configParseObj.read(args)
    return configParseObj



for eqScenario in ['sim6p8_cr2022_rlz_1']:
    for retrofitPrefix in ['b0','r1','r2']:
        for view in ['casualties_agg_view',
        'damage_state_agg_view',
        'economic_loss_agg_view',
        #'full_retrofit_agg_view',
        'functional_state_agg_view',
        #'partial_retrofit_agg_view',
        'recovery_agg_view',
        'scenario_hazard_agg_view',
        'scenario_hazard_threat_agg_view',
        'scenario_rupture_agg_view',
        'social_disruption_agg_view']:
            indexName = 'dsra_{eqScenario}_{retrofitPrefix}_{view}'.format(**{'eqScenario':eqScenario, 'retrofitPrefix':retrofitPrefix, 'view':view})
            viewIndex = es.indices.get(index=indexName)
            list = []
            for field in viewIndex[indexName]['mappings']['properties']['properties']['properties']:
                list.append(field)
            for field in list:
                print('Calculating break points for: '+'dsra_{eqScenario}_{retrofitPrefix}_{view}: {field}'.format(**{'eqScenario':eqScenario, 'retrofitPrefix':retrofitPrefix, 'view':view, 'field':field}))
                try:
                    os.system('python3 caclulateJenks.py --eqScenario={eqScenario} --retrofitPrefix={retrofitPrefix} --dbview={view} --field={field} --bins=5'.format(**{'eqScenario':eqScenario, 'retrofitPrefix':retrofitPrefix, 'view':view}))