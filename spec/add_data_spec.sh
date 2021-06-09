#shellcheck shell=bash

Describe 'Check add_data.sh'
  Include python/add_data.sh
  Include sample.env
  ADD_DATA_PRINT_LINENO=false
  ADD_DATA_PRINT_FUNCNAME=false

  It 'checks LOG'
    When call LOG Hello
    The stdout should eq '[add_data] Hello'
  End

  It 'checks environment variables'
    When call check_environment_variables
    The stdout should equal "
[add_data] ## Check needed environment variables
[add_data] INFO: POSTGRES_USER = postgres
[add_data] INFO: POSTGRES_PASS = password
[add_data] INFO: POSTGRES_PORT = 5432
[add_data] INFO: POSTGRES_HOST = db-opendrr
[add_data] INFO: DB_NAME = opendrr
[add_data] INFO: POPULATE_DB = 0
[add_data] INFO: KIBANA_ENDPOINT = http://kibana-opendrr:5601
[add_data] INFO: ES_ENDPOINT = http://elasticsearch-opendrr:9200
[add_data] INFO: ES_USER = 
[add_data] WARNING: Optional variable ES_USER is not set.  Continuing...
[add_data] INFO: ES_PASS = 
[add_data] WARNING: Optional variable ES_PASS is not set.  Continuing...
[add_data] INFO: loadDsraScenario = true
[add_data] INFO: loadPsraModels = true
[add_data] INFO: loadHazardThreat = false
[add_data] INFO: loadPhysicalExposure = true
[add_data] INFO: loadRiskDynamics = true
[add_data] INFO: loadSocialFabric = true"
  End
End
