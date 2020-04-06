# =================================================================
#
# Authors: Drew Rotheram <drew.rotheram@gmail.com>
#
# =================================================================


import os

for eqScenario in ['sim6p8_cr2022_rlz_1']:
    for retrofitPrefix in ['b0']: #,'r1','r2']:
        for view in ['casualties_agg_view',
        'damage_state_agg_view',
        'economic_loss_agg_view',
        'full_retrofit_agg_view',
        'functional_state_agg_view',
        'partial_retrofit_agg_view',
        'recovery_agg_view',
        'scenario_hazard_agg_view',
        'scenario_hazard_threat_agg_view',
        'scenario_rupture_agg_view',
        'social_disruption_agg_view']:
            print('loading: '+'dsra_{eqScenario}_{retrofitPrefix}_{view}.json'.format(**{'eqScenario':eqScenario, 'retrofitPrefix':retrofitPrefix, 'view':view}))
            os.system('python load_es_data.py dsra_{eqScenario}_{retrofitPrefix}_{view}.json "Sauid"'.format(**{'eqScenario':eqScenario, 'retrofitPrefix':retrofitPrefix, 'view':view}))