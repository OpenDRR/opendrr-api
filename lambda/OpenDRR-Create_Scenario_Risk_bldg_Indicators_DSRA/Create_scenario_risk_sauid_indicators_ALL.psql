-- create schema for new scenario
CREATE SCHEMA IF NOT EXISTS results_dsra_{eqscenario};

-- create scenario risk building indicators
DROP VIEW IF EXISTS results_dsra_{eqscenario}.dsra_{eqscenario}_{realization}_scenario_shakemap_intensity_sauid;
CREATE VIEW results_dsra_{eqscenario}.dsra_{eqscenario}_{realization}_scenario_shakemap_intensity_sauid AS 

SELECT
b.sauid AS "Sauid",

-- 3.1.1 Scenario Shakemap Intensity
f.rupture_name AS "sH_RupName",
a."Rupture_Abbr" AS "sH_RupAbbr",
f.source_type AS "sH_Source",
f.magnitude AS "sH_Mag",
CAST(CAST(ROUND(CAST(f.lon AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_HypoLon",
CAST(CAST(ROUND(CAST(f.lat AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_HypoLat",
CAST(CAST(ROUND(CAST(f.depth AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_HypoDepth",
f.rake AS "sH_Rake",
a."gmpe_Model" AS "sH_GMPE",
a."Realization" AS "sH_Rlz",
CAST(CAST(ROUND(CAST(a."Weight" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_Wght",
e.site_id AS "sH_SiteID",
CAST(CAST(ROUND(CAST(e.lon AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "SiteID_Lon",
CAST(CAST(ROUND(CAST(e.lat AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "SiteID_Lat",
CAST(CAST(ROUND(CAST(d.vs_lon AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS  "sH_Vs30Lon",
CAST(CAST(ROUND(CAST(d.vs_lat AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_Vs30Lat",
CAST(CAST(ROUND(CAST(d.vs30 AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_Vs30",
CAST(CAST(ROUND(CAST(0 AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_Vs1p0", --add later
CAST(CAST(ROUND(CAST(0 AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_V2p5", -- add later
CAST(CAST(ROUND(CAST(e."gmv_pgv" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_PGV",
CAST(CAST(ROUND(CAST(e."gmv_pga" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_PGA",
CAST(CAST(ROUND(CAST(e."gmv_SA(0.2)" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_Sa0p2",
CAST(CAST(ROUND(CAST(e."gmv_SA(0.3)" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_Sa0p3",
CAST(CAST(ROUND(CAST(e."gmv_SA(0.6)" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_Sa0p6",
CAST(CAST(ROUND(CAST(e."gmv_SA(1.0)" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_Sa1p0",
CAST(CAST(ROUND(CAST(0 AS NUMERIC),6) AS FLOAT) AS NUMERIC)AS "sH_Sa2p0",  --"Spectral Acceleration (2.0s)" - change source to reflect 2.0 later

i.geom AS "geom_poly",
i.geompoint AS "geom_point"

FROM dsra.dsra_{eqscenario}_{realization} a
LEFT JOIN exposure.canada_exposure b ON a."AssetID" = b.id 
LEFT JOIN lut.retrofit_costs c ON b.eqbldgtype = c."Eq_BldgType"
LEFT JOIN vs30.vs30_bc_site_model_xref d ON a."AssetID" = d.id
LEFT JOIN gmf.gmfdata_sitemesh_{eqscenario}_37_xref e ON b.id = e.id
LEFT JOIN ruptures.rupture_table f ON f.rupture_name = a."Rupture_Abbr"
LEFT JOIN lut.collapse_probability g ON b.eqbldgtype = g.eqbldgtype
LEFT JOIN census.census_2016_canada h ON b.sauid = h.sauidt
LEFT JOIN boundaries."Geometry SAUID" i ON b.sauid = i."SAUIDt"
LEFT JOIN sovi.sovi_census_canada j ON b.sauid = j.sauidt
LEFT JOIN sovi.sovi_index_canada k ON b.sauid = k.sauidt
GROUP BY a."Rupture_Abbr",a."gmpe_Model",a."Weight",a."Realization",b.sauid,d.vs30,d.vs_lon,d.vs_lat,e.site_id,e.lon,e.lat,f.source_type,
f.rupture_name,f.magnitude,f.lon,f.lat,f.depth,f.rake,b.lon,b.lat,e."gmv_pgv",e."gmv_pga",e."gmv_SA(0.2)",e."gmv_SA(0.6)",
e."gmv_SA(1.0)",e."gmv_SA(0.3)",h."area_km2",h.area_ha,h.censuspop,h.censusbldg,h.censusdu,i.geom,i.geompoint;

-- create schema for new scenario
CREATE SCHEMA IF NOT EXISTS results_dsra_{eqscenario};

-- create scenario risk building indicators
DROP VIEW IF EXISTS results_dsra_{eqscenario}.dsra_{eqscenario}_{realization}_damage_state_sauid;
CREATE VIEW results_dsra_{eqscenario}.dsra_{eqscenario}_{realization}_damage_state_sauid AS 

-- 3.0 Earthquake Scenario Risk (DSRA)
-- 3.2 Building Performance
SELECT 
b.sauid AS "Sauid",

-- 3.2.1 Damage State - b0
CAST(CAST(ROUND(CAST(SUM(a."sD_None_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_None_b0",
CAST(CAST(ROUND(CAST(AVG(a."sD_None_b0" / b.number) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtr_None_b0",
CAST(CAST(ROUND(CAST(AVG(a."sD_None_stdv_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtsd_None_b0",

CAST(CAST(ROUND(CAST(SUM(a."sD_Slight_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Slight_b0",
CAST(CAST(ROUND(CAST(AVG(a."sD_Slight_b0" / b.number) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtr_Slight_b0",
CAST(CAST(ROUND(CAST(AVG(a."sD_Slight_stdv_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtsd_Slight_b0",

CAST(CAST(ROUND(CAST(SUM(a."sD_Moderate_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Moderate_b0",
CAST(CAST(ROUND(CAST(AVG(a."sD_Moderate_b0" / b.number) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtr_Moderate_b0",
CAST(CAST(ROUND(CAST(AVG(a."sD_Moderate_stdv_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtsd_Moderate_b0",

CAST(CAST(ROUND(CAST(SUM(a."sD_Extensive_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Extensive_b0",
CAST(CAST(ROUND(CAST(AVG(a."sD_Extensive_b0" / b.number) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtr_Extensive_b0",
CAST(CAST(ROUND(CAST(AVG(a."sD_Extensive_stdv_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtsd_Extensive_b0",

CAST(CAST(ROUND(CAST(SUM(a."sD_Complete_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Complete_b0",
CAST(CAST(ROUND(CAST(AVG(a."sD_Complete_b0" / b.number) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtr_Complete_b0",
CAST(CAST(ROUND(CAST(AVG(a."sD_Complete_stdv_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtsd_Complete_b0",

CAST(CAST(ROUND(CAST(SUM(a."sD_Collapse_b0" * b.number) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sD_Collapse_b0",
CAST(CAST(ROUND(CAST(AVG(a."sD_Collapse_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtr_Collapse_b0",
CAST(CAST(ROUND(CAST(AVG(a."sD_Complete_stdv_b0" * g.collapse_pc) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtsd_Collapse_b0",

-- 3.2.1 Damage State - r2
CAST(CAST(ROUND(CAST(SUM(a."sD_None_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_None_r2",
CAST(CAST(ROUND(CAST(AVG(a."sD_None_r2" / b.number) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtr_None_r2",
CAST(CAST(ROUND(CAST(AVG(a."sD_None_stdv_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtsd_None_r2",

CAST(CAST(ROUND(CAST(SUM(a."sD_Slight_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Slight_r2",
CAST(CAST(ROUND(CAST(AVG(a."sD_Slight_r2" / b.number) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtr_Slight_r2",
CAST(CAST(ROUND(CAST(AVG(a."sD_Slight_stdv_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtsd_Slight_r2",

CAST(CAST(ROUND(CAST(SUM(a."sD_Moderate_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Moderate_r2",
CAST(CAST(ROUND(CAST(AVG(a."sD_Moderate_r2" / b.number) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtr_Moderate_r2",
CAST(CAST(ROUND(CAST(AVG(a."sD_Moderate_stdv_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtsd_Moderate_r2",

CAST(CAST(ROUND(CAST(SUM(a."sD_Extensive_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Extensive_r2",
CAST(CAST(ROUND(CAST(AVG(a."sD_Extensive_r2" / b.number) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtr_Extensive_r2",
CAST(CAST(ROUND(CAST(AVG(a."sD_Extensive_stdv_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtsd_Extensive_r2",

CAST(CAST(ROUND(CAST(SUM(a."sD_Complete_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Complete_r2",
CAST(CAST(ROUND(CAST(AVG(a."sD_Complete_r2" / b.number) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtr_Complete_r2",
CAST(CAST(ROUND(CAST(AVG(a."sD_Complete_stdv_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtsd_Complete_r2",

CAST(CAST(ROUND(CAST(SUM(a."sD_Collapse_r2" * b.number) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sD_Collapse_r2",
CAST(CAST(ROUND(CAST(AVG(a."sD_Collapse_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtr_Collapse_r2",
CAST(CAST(ROUND(CAST(AVG(a."sD_Complete_stdv_r2" * g.collapse_pc) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDtsd_Collapse_r2",

i.geom AS "geom_poly",
i.geompoint AS "geom_point"

FROM dsra.dsra_{eqscenario}_{realization} a
LEFT JOIN exposure.canada_exposure b ON a."AssetID" = b.id 
LEFT JOIN lut.retrofit_costs c ON b.eqbldgtype = c."Eq_BldgType"
LEFT JOIN vs30.vs30_bc_site_model_xref d ON a."AssetID" = d.id
LEFT JOIN gmf.gmfdata_sitemesh_{eqscenario}_37_xref e ON b.id = e.id
LEFT JOIN ruptures.rupture_table f ON f.rupture_name = a."Rupture_Abbr"
LEFT JOIN lut.collapse_probability g ON b.eqbldgtype = g.eqbldgtype
LEFT JOIN census.census_2016_canada h ON b.sauid = h.sauidt
LEFT JOIN boundaries."Geometry SAUID" i ON b.sauid = i."SAUIDt"
LEFT JOIN sovi.sovi_census_canada j ON b.sauid = j.sauidt
LEFT JOIN sovi.sovi_index_canada k ON b.sauid = k.sauidt
GROUP BY b.sauid,i.geom,i.geompoint;


-- create scenario risk building indicators
DROP VIEW IF EXISTS results_dsra_{eqscenario}.dsra_{eqscenario}_{realization}_recovery_time_sauid;
CREATE VIEW results_dsra_{eqscenario}.dsra_{eqscenario}_{realization}_recovery_time_sauid AS 

-- 3.0 Earthquake Scenario Risk (DSRA)
-- 3.2 Building Performance
SELECT 
b.sauid AS "Sauid",

-- 3.2.1 Recovery Time - b0
CAST(CAST(ROUND(CAST(SUM(a."sD_None_b0" + a."sD_Slight_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_GreenTag_b_b0",
CAST(CAST(ROUND(CAST(SUM(a."sD_None_b0" + a."sD_Slight_b0")/5 AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_GreenTag_i_b0",
CAST(CAST(ROUND(CAST(SUM(a."sD_Extensive_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_YellowTag_d_b0",
CAST(CAST(ROUND(CAST(SUM(a."sD_Extensive_b0")/2.5 AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_YellowTag_i_b0",
CAST(CAST(ROUND(CAST(SUM(a."sD_Complete_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_RedTag_b_b0",
CAST(CAST(ROUND(CAST(SUM(a."sD_Complete_b0")/5 AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_RedTag_i_b0" ,
CAST(CAST(ROUND(CAST(SUM(a."sD_None_b0" + (a."sD_Slight_b0"*0.2) +(a."sD_Moderate_b0"*0.05)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Operational_b0",
CAST(CAST(ROUND(CAST(SUM((a."sD_Slight_b0"*0.8) + (a."sD_Moderate_b0"*0.75) + (a."sD_Extensive_b0"*0.2)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Functional_b0",
CAST(CAST(ROUND(CAST(SUM((a."sD_Moderate_b0"*0.2) + (a."sD_Extensive_b0"*0.4)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Repairable_b0",
CAST(CAST(ROUND(CAST(SUM((a."sD_Extensive_b0"*0.3) + (a."sD_Complete_b0"*0.2)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Failure_b0",
CAST(CAST(ROUND(CAST(AVG(a."sC_Repair_b0")/(AVG(b.number)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCm_Repair_b0",
CAST(CAST(ROUND(CAST(AVG(a."sC_Construxn_b0")/(AVG(b.number)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "SCm_Construxn_b0",
CAST(CAST(ROUND(CAST(AVG(a."sC_Downtime_b0")/(AVG(b.number)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCm_Downtime_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_DebrisBW_b0" + a."sC_DebrisC_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_DebrisTotal_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_DebrisBW_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_DebrisBW_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_DebrisC_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_DebrisCS_b0",

-- 3.2.1 Recovery Time - r2
CAST(CAST(ROUND(CAST(SUM(a."sD_None_r2" + a."sD_Slight_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_GreenTag_b_r2",
CAST(CAST(ROUND(CAST(SUM(a."sD_None_r2" + a."sD_Slight_r2")/5 AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_GreenTag_i_r2",
CAST(CAST(ROUND(CAST(SUM(a."sD_Extensive_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_YellowTag_d_r2",
CAST(CAST(ROUND(CAST(SUM(a."sD_Extensive_r2")/2.5 AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_YellowTag_i_r2",
CAST(CAST(ROUND(CAST(SUM(a."sD_Complete_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_RedTag_b_r2",
CAST(CAST(ROUND(CAST(SUM(a."sD_Complete_r2")/5 AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_RedTag_i_r2" ,
CAST(CAST(ROUND(CAST(SUM(a."sD_None_r2" + (a."sD_Slight_r2"*0.2) +(a."sD_Moderate_r2"*0.05)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Operational_r2",
CAST(CAST(ROUND(CAST(SUM((a."sD_Slight_r2"*0.8) + (a."sD_Moderate_r2"*0.75) + (a."sD_Extensive_r2"*0.2)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Functional_r2",
CAST(CAST(ROUND(CAST(SUM((a."sD_Moderate_r2"*0.2) + (a."sD_Extensive_r2"*0.4)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Repairable_r2",
CAST(CAST(ROUND(CAST(SUM((a."sD_Extensive_r2"*0.3) + (a."sD_Complete_r2"*0.2)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDt_Failure_r2",
CAST(CAST(ROUND(CAST(AVG(a."sC_Repair_r2")/(AVG(b.number)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCm_Repair_r2",
CAST(CAST(ROUND(CAST(AVG(a."sC_Construxn_r2")/(AVG(b.number)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "SCm_Construxn_r2",
CAST(CAST(ROUND(CAST(AVG(a."sC_Downtime_r2")/(AVG(b.number)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCm_Downtime_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_DebrisBW_r2" + a."sC_DebrisC_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_DebrisTotal_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_DebrisBW_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_DebrisBW_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_DebrisC_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_DebrisCS_r2",


i.geom AS "geom_poly",
i.geompoint AS "geom_point"

FROM dsra.dsra_{eqscenario}_{realization} a
LEFT JOIN exposure.canada_exposure b ON a."AssetID" = b.id 
LEFT JOIN lut.retrofit_costs c ON b.eqbldgtype = c."Eq_BldgType"
LEFT JOIN vs30.vs30_bc_site_model_xref d ON a."AssetID" = d.id
LEFT JOIN gmf.gmfdata_sitemesh_{eqscenario}_37_xref e ON b.id = e.id
LEFT JOIN ruptures.rupture_table f ON f.rupture_name = a."Rupture_Abbr"
LEFT JOIN lut.collapse_probability g ON b.eqbldgtype = g.eqbldgtype
LEFT JOIN census.census_2016_canada h ON b.sauid = h.sauidt
LEFT JOIN boundaries."Geometry SAUID" i ON b.sauid = i."SAUIDt"
LEFT JOIN sovi.sovi_census_canada j ON b.sauid = j.sauidt
LEFT JOIN sovi.sovi_index_canada k ON b.sauid = k.sauidt
GROUP BY b.sauid,i.geom,i.geompoint;

-- create schema for new scenario
CREATE SCHEMA IF NOT EXISTS results_dsra_{eqscenario};

-- create scenario risk building indicators
DROP VIEW IF EXISTS results_dsra_{eqscenario}.dsra_{eqscenario}_{realization}_casualties_sauid;
CREATE VIEW results_dsra_{eqscenario}.dsra_{eqscenario}_{realization}_casualties_sauid AS 

-- 3.0 Earthquake Scenario Risk (DSRA)
-- 3.3 Affected People
SELECT 
b.sauid AS "Sauid",

-- 3.3.1 Casualties - b0
CAST(CAST(ROUND(CAST(SUM(a."sL_Fatalities_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLt_Fatality_b0",
CAST(CAST(ROUND(CAST(AVG(a."sL_Fatalities_stdv_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLtsd_Fatality_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasDayL1_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasDay_min_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasDayL2_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasDay_mod_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasDayL3_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasDay_ser_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasDayL4_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasDay_crit_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasNightL1_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasNit_min_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasNightL2_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasNit_mod_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasNightL3_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasNit_ser_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasNightL4_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasNit_crit_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasTransitL1_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasTrn_min_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasTransitL2_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasTrn_mod_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasTransitL3_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasTrn_ser_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasTransitL4_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasTrn_crit_b0",

-- 3.3.1 Casualties - r2
CAST(CAST(ROUND(CAST(SUM(a."sL_Fatalities_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLt_Fatality_r2",
CAST(CAST(ROUND(CAST(AVG(a."sL_Fatalities_stdv_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLtsd_Fatality_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasDayL1_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasDay_min_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasDayL2_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasDay_mod_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasDayL3_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasDay_ser_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasDayL4_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasDay_crit_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasNightL1_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasNit_min_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasNightL2_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasNit_mod_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasNightL3_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasNit_ser_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasNightL4_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasNit_crit_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasTransitL1_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasTrn_min_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasTransitL2_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasTrn_mod_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasTransitL3_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasTrn_ser_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_CasTransitL4_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_CasTrn_crit_r2",

i.geom AS "geom_poly",
i.geompoint AS "geom_point"

FROM dsra.dsra_{eqscenario}_{realization} a
LEFT JOIN exposure.canada_exposure b ON a."AssetID" = b.id 
LEFT JOIN lut.retrofit_costs c ON b.eqbldgtype = c."Eq_BldgType"
LEFT JOIN vs30.vs30_bc_site_model_xref d ON a."AssetID" = d.id
LEFT JOIN gmf.gmfdata_sitemesh_{eqscenario}_37_xref e ON b.id = e.id
LEFT JOIN ruptures.rupture_table f ON f.rupture_name = a."Rupture_Abbr"
LEFT JOIN lut.collapse_probability g ON b.eqbldgtype = g.eqbldgtype
LEFT JOIN census.census_2016_canada h ON b.sauid = h.sauidt
LEFT JOIN boundaries."Geometry SAUID" i ON b.sauid = i."SAUIDt"
LEFT JOIN sovi.sovi_census_canada j ON b.sauid = j.sauidt
LEFT JOIN sovi.sovi_index_canada k ON b.sauid = k.sauidt
GROUP BY b.sauid,i.geom,i.geompoint;


-- create scenario risk building indicators
DROP VIEW IF EXISTS results_dsra_{eqscenario}.dsra_{eqscenario}_{realization}_social_disruption_sauid;
CREATE VIEW results_dsra_{eqscenario}.dsra_{eqscenario}_{realization}_social_disruption_sauid AS 

-- 3.0 Earthquake Scenario Risk (DSRA)
-- 3.3 Affected People
SELECT 
b.sauid AS "Sauid",

-- 3.3.2 Social Disruption - b0
CAST(CAST(ROUND(CAST(((0.73 * COALESCE((CASE WHEN j.inc_hshld <= 15000 THEN 0.62 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 15000 AND j.inc_hshld <= 20000 THEN 0.42 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 20000 AND j.inc_hshld <= 35000 THEN 0.29 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 35000 AND j.inc_hshld <= 50000 THEN 0.22 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 50000 THEN 0.13 ELSE 0 END),0)) + 
(0.27 * COALESCE(j.imm_lt5 * 0.24,0) + COALESCE(j.live_alone * 0.48,0) + COALESCE(j.no_engfr * 0.47,0) + COALESCE(j.lonepar3kids * 0.26,0) +
COALESCE(j.indigenous * 0.26,0))) *
(COALESCE(((SUM(COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_b0" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_b0" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0))) * h.censuspop) / NULLIF(h.censusdu,0),0)) *
(COALESCE((CASE WHEN j.inc_hshld <= 15000 THEN 0.62 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 15000 AND j.inc_hshld <= 20000 THEN 0.42 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 20000 AND j.inc_hshld <= 35000 THEN 0.29 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 35000 AND j.inc_hshld <= 50000 THEN 0.22 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 50000 THEN 0.13 ELSE 0 END),0)) * 
(COALESCE(j.imm_lt5 * 0.24,0) + COALESCE(j.live_alone * 0.48,0) + COALESCE(j.no_engfr * 0.47,0) + COALESCE(j.lonepar3kids * 0.26,0) +
COALESCE(j.indigenous * 0.26,0)) * 
(COALESCE(j.renter * 0.40,0) + COALESCE(((h.censusdu * h.people_du) - j.renter) * 0.40,0)) * 
(COALESCE(j.age_gt65 * 0.40,0) + COALESCE(j.age_lt6 * 0.40,0)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Shelter_b0",

CAST(CAST(ROUND(CAST(SUM(a."sC_DisplRes_3_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Res3_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_DisplRes_30_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Res30_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_DisplRes_90_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Res90_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_DisplRes_180_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Res180_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_DisplRes_360_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Res360_b0",

(SUM(COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_b0" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_b0" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0))) AS "sCt_Hshld_b0",

CAST(CAST(ROUND(CAST(SUM(CASE WHEN a."sC_Downtime_b0" > 3 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_b0" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_b0" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Hshld3_b0",

CAST(CAST(ROUND(CAST(SUM(CASE WHEN a."sC_Downtime_b0" > 30 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_b0" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_b0" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_Hshld30_b0",

CAST(CAST(ROUND(CAST(SUM(CASE WHEN a."sC_Downtime_b0" > 90 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_b0" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_b0" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Hshld90_b0",

CAST(CAST(ROUND(CAST(SUM(CASE WHEN a."sC_Downtime_b0" > 180 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_b0" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_b0" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Hshld180_b0",

CAST(CAST(ROUND(CAST(SUM(CASE WHEN a."sC_Downtime_b0" > 360 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_b0" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_b0" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Hshld360_b0",

CAST(CAST(ROUND(CAST(SUM(a."sC_DisrupEmpl_30_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Empl30_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_DisrupEmpl_90_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Empl90_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_DisrupEmpl_180_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Empl180_b0",
CAST(CAST(ROUND(CAST(SUM(a."sC_DisrupEmpl_360_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Empl360_b0",

-- 3.3.2 Social Disruption - r2
CAST(CAST(ROUND(CAST(((0.73 * COALESCE((CASE WHEN j.inc_hshld <= 15000 THEN 0.62 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 15000 AND j.inc_hshld <= 20000 THEN 0.42 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 20000 AND j.inc_hshld <= 35000 THEN 0.29 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 35000 AND j.inc_hshld <= 50000 THEN 0.22 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 50000 THEN 0.13 ELSE 0 END),0)) + 
(0.27 * COALESCE(j.imm_lt5 * 0.24,0) + COALESCE(j.live_alone * 0.48,0) + COALESCE(j.no_engfr * 0.47,0) + COALESCE(j.lonepar3kids * 0.26,0) +
COALESCE(j.indigenous * 0.26,0))) *
(COALESCE(((SUM(COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_r2" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_r2" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0))) * h.censuspop) / NULLIF(h.censusdu,0),0)) *
(COALESCE((CASE WHEN j.inc_hshld <= 15000 THEN 0.62 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 15000 AND j.inc_hshld <= 20000 THEN 0.42 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 20000 AND j.inc_hshld <= 35000 THEN 0.29 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 35000 AND j.inc_hshld <= 50000 THEN 0.22 ELSE 0 END),0) + 
COALESCE((CASE WHEN j.inc_hshld > 50000 THEN 0.13 ELSE 0 END),0)) * 
(COALESCE(j.imm_lt5 * 0.24,0) + COALESCE(j.live_alone * 0.48,0) + COALESCE(j.no_engfr * 0.47,0) + COALESCE(j.lonepar3kids * 0.26,0) +
COALESCE(j.indigenous * 0.26,0)) * 
(COALESCE(j.renter * 0.40,0) + COALESCE(((h.censusdu * h.people_du) - j.renter) * 0.40,0)) * 
(COALESCE(j.age_gt65 * 0.40,0) + COALESCE(j.age_lt6 * 0.40,0)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Shelter_r2",

CAST(CAST(ROUND(CAST(SUM(a."sC_DisplRes_3_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Res3_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_DisplRes_30_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Res30_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_DisplRes_90_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Res90_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_DisplRes_180_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Res180_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_DisplRes_360_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Res360_r2",

(SUM(COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_r2" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_r2" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0))) AS "sCt_Hshld_r2",

CAST(CAST(ROUND(CAST(SUM(CASE WHEN a."sC_Downtime_r2" > 3 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_r2" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_r2" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Hshld3_r2",

CAST(CAST(ROUND(CAST(SUM(CASE WHEN a."sC_Downtime_r2" > 30 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_r2" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_r2" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_Hshld30_r2",

CAST(CAST(ROUND(CAST(SUM(CASE WHEN a."sC_Downtime_r2" > 90 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_r2" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_r2" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Hshld90_r2",

CAST(CAST(ROUND(CAST(SUM(CASE WHEN a."sC_Downtime_r2" > 180 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_r2" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_r2" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Hshld180_r2",

CAST(CAST(ROUND(CAST(SUM(CASE WHEN a."sC_Downtime_r2" > 360 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_r2" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_r2" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Hshld360_r2",

CAST(CAST(ROUND(CAST(SUM(a."sC_DisrupEmpl_30_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Empl30_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_DisrupEmpl_90_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Empl90_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_DisrupEmpl_180_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Empl180_r2",
CAST(CAST(ROUND(CAST(SUM(a."sC_DisrupEmpl_360_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sCt_Empl360_r2",

i.geom AS "geom_poly",
i.geompoint AS "geom_point"

FROM dsra.dsra_{eqscenario}_{realization} a
LEFT JOIN exposure.canada_exposure b ON a."AssetID" = b.id 
LEFT JOIN lut.retrofit_costs c ON b.eqbldgtype = c."Eq_BldgType"
LEFT JOIN vs30.vs30_bc_site_model_xref d ON a."AssetID" = d.id
LEFT JOIN gmf.gmfdata_sitemesh_{eqscenario}_37_xref e ON b.id = e.id
LEFT JOIN ruptures.rupture_table f ON f.rupture_name = a."Rupture_Abbr"
LEFT JOIN lut.collapse_probability g ON b.eqbldgtype = g.eqbldgtype
LEFT JOIN census.census_2016_canada h ON b.sauid = h.sauidt
LEFT JOIN boundaries."Geometry SAUID" i ON b.sauid = i."SAUIDt"
LEFT JOIN sovi.sovi_census_canada j ON b.sauid = j.sauidt
LEFT JOIN sovi.sovi_index_canada k ON b.sauid = k.sauidt
GROUP BY b.sauid,h.censuspop,h.censusdu,h.people_du,j.inc_hshld,j.imm_lt5,j.live_alone,j.no_engfr,j.lonepar3kids,j.indigenous,j.renter,j.age_lt6,j.age_gt65,i.geom,i.geompoint;

-- create schema for new scenario
CREATE SCHEMA IF NOT EXISTS results_dsra_{eqscenario};

-- create scenario risk building indicators
DROP VIEW IF EXISTS results_dsra_{eqscenario}.dsra_{eqscenario}_{realization}_economic_loss_sauid;
CREATE VIEW results_dsra_{eqscenario}.dsra_{eqscenario}_{realization}_economic_loss_sauid AS 

-- 3.0 Earthquake Scenario Risk (DSRA)
-- 3.4 Economic Security
SELECT
b.sauid AS "Sauid",

-- 3.4.1 Economic Loss - b0
CAST(CAST(ROUND(CAST(SUM(a."sL_Str_b0" + a."sL_NStr_b0" + a."sL_Cont_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLt_Asset_b0",
CAST(CAST(ROUND(CAST(SUM(a."sL_Str_b0" + a."sL_NStr_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLt_Bldg_b0",
CAST(CAST(ROUND(CAST((COALESCE((AVG(a."sL_Str_b0" + a."sL_NStr_b0"))/ NULLIF(AVG((a."sL_Str_b0" + a."sL_NStr_b0" + a."sL_Cont_b0")),0),0)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLmr_Bldg_b0",
CAST(CAST(ROUND(CAST(SUM(a."sL_Str_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLt_Str_b0",
CAST(CAST(ROUND(CAST(SUM(a."sL_Str_stdv_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLmsd_Str_b0",
CAST(CAST(ROUND(CAST(SUM(a."sL_NStr_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLt_NStr_b0",
CAST(CAST(ROUND(CAST(SUM(a."sL_NStr_stdv_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLmsd_NStr_b0",
CAST(CAST(ROUND(CAST(SUM(a."sL_Cont_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLt_Cont_b0",
CAST(CAST(ROUND(CAST(SUM(a."sL_Cont_stdv_b0") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLmsd_Cont_b0",

-- 3.4.1 Economic Loss - r2
CAST(CAST(ROUND(CAST(SUM(a."sL_Str_r2" + a."sL_NStr_r2" + a."sL_Cont_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLt_Asset_r2",
CAST(CAST(ROUND(CAST(SUM(a."sL_Str_r2" + a."sL_NStr_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLt_Bldg_r2",
CAST(CAST(ROUND(CAST((COALESCE((AVG(a."sL_Str_r2" + a."sL_NStr_r2"))/ NULLIF(AVG((a."sL_Str_r2" + a."sL_NStr_r2" + a."sL_Cont_r2")),0),0)) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLmr_Bldg_r2",
CAST(CAST(ROUND(CAST(SUM(a."sL_Str_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLt_Str_r2",
CAST(CAST(ROUND(CAST(SUM(a."sL_Str_stdv_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLmsd_Str_r2",
CAST(CAST(ROUND(CAST(SUM(a."sL_NStr_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLt_NStr_r2",
CAST(CAST(ROUND(CAST(SUM(a."sL_NStr_stdv_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLmsd_NStr_r2",
CAST(CAST(ROUND(CAST(SUM(a."sL_Cont_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLt_Cont_r2",
CAST(CAST(ROUND(CAST(SUM(a."sL_Cont_stdv_r2") AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLmsd_Cont_r2",

CAST(CAST(ROUND(CAST(CASE WHEN (AVG(((a."sL_Str_b0" + a."sL_NStr_b0" + a."sL_Cont_b0") - (a."sL_Str_r2" + a."sL_NStr_r2" + a."sL_Cont_r2"))/(c."CAD_RetrofitCost_Bldg"))) > 0
THEN (AVG(((a."sL_Str_b0" + a."sL_NStr_b0" + a."sL_Cont_b0") - (a."sL_Str_r2" + a."sL_NStr_r2" + a."sL_Cont_r2"))/(c."CAD_RetrofitCost_Bldg"))) ELSE 1 END AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLmr2_BCR" ,

CAST(CAST(ROUND(CAST(CASE WHEN (AVG(((a."sL_Str_b0" + a."sL_NStr_b0" + a."sL_Cont_b0") - (a."sL_Str_r2" + a."sL_NStr_r2" + a."sL_Cont_r2")) * ((EXP(-0.03*100)/0.03)/(c."CAD_RetrofitCost_Bldg")))) > 0
THEN (AVG(((a."sL_Str_b0" + a."sL_NStr_b0" + a."sL_Cont_b0") - (a."sL_Str_r2" + a."sL_NStr_r2" + a."sL_Cont_r2")) * ((EXP(-0.03*100)/0.03)/(c."CAD_RetrofitCost_Bldg")))) ELSE 1 END AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLmr2_RoI",

i.geom AS "geom_poly",
i.geompoint AS "geom_point"

FROM dsra.dsra_{eqscenario}_{realization} a
LEFT JOIN exposure.canada_exposure b ON a."AssetID" = b.id 
LEFT JOIN lut.retrofit_costs c ON b.eqbldgtype = c."Eq_BldgType"
LEFT JOIN vs30.vs30_bc_site_model_xref d ON a."AssetID" = d.id
LEFT JOIN gmf.gmfdata_sitemesh_{eqscenario}_37_xref e ON b.id = e.id
LEFT JOIN ruptures.rupture_table f ON f.rupture_name = a."Rupture_Abbr"
LEFT JOIN lut.collapse_probability g ON b.eqbldgtype = g.eqbldgtype
LEFT JOIN census.census_2016_canada h ON b.sauid = h.sauidt
LEFT JOIN boundaries."Geometry SAUID" i ON b.sauid = i."SAUIDt"
LEFT JOIN sovi.sovi_census_canada j ON b.sauid = j.sauidt
LEFT JOIN sovi.sovi_index_canada k ON b.sauid = k.sauidt
GROUP BY b.sauid,f.source_type,f.rupture_name,f.magnitude,i.geom,i.geompoint;