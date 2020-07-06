
-- create schema for new scenario
CREATE SCHEMA IF NOT EXISTS results_dsra_sim6p8_cr2022;

-- create scenario risk building indicators

DROP VIEW IF EXISTS results_dsra_sim6p8_cr2022.dsra_sim6p8_cr2022_rlz_1_scenario_shakemap_intensity_building CASCADE;
CREATE VIEW results_dsra_sim6p8_cr2022.dsra_sim6p8_cr2022_rlz_1_scenario_shakemap_intensity_building AS 

-- 3.0 Earthquake Scenario Risk (DSRA)
-- 3.1 Earthquake Hazard
SELECT 
a."AssetID",
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
CAST(CAST(ROUND(CAST(d.z1pt0 AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_Vs1p0",
CAST(CAST(ROUND(CAST(d.z2pt5 AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_Vs2p5",
CAST(CAST(ROUND(CAST(e."gmv_pgv" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_PGV",
CAST(CAST(ROUND(CAST(e."gmv_pga" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_PGA",
CAST(CAST(ROUND(CAST(e."gmv_SA(0.2)" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_Sa0p2",
CAST(CAST(ROUND(CAST(e."gmv_SA(0.3)" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_Sa0p3",
CAST(CAST(ROUND(CAST(e."gmv_SA(0.6)" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_Sa0p6",
CAST(CAST(ROUND(CAST(e."gmv_SA(1.0)" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sH_Sa1p0",
CAST(CAST(ROUND(CAST(e."gmv_SA(2.0)" AS NUMERIC),6) AS FLOAT) AS NUMERIC)AS "sH_Sa2p0",

b.geom AS "geom_point"

FROM dsra.dsra_sim6p8_cr2022_rlz_1 a
LEFT JOIN exposure.canada_exposure b ON a."AssetID" = b.id 
LEFT JOIN lut.retrofit_costs c ON b.eqbldgtype = c."Eq_BldgType"
LEFT JOIN vs30.vs30_bc_site_model_xref d ON a."AssetID" = d.id
LEFT JOIN gmf.gmfdata_sitemesh_sim6p8_cr2022_37_xref e ON b.id = e.id
LEFT JOIN ruptures.rupture_table f ON f.rupture_name = a."Rupture_Abbr"
LEFT JOIN lut.collapse_probability g ON b.eqbldgtype = g.eqbldgtype
LEFT JOIN census.census_2016_canada h ON b.sauid = h.sauidt;

-- create schema for new scenario
CREATE SCHEMA IF NOT EXISTS results_dsra_sim6p8_cr2022;

-- create scenario risk building indicators
DROP VIEW IF EXISTS results_dsra_sim6p8_cr2022.dsra_sim6p8_cr2022_rlz_1_damage_state_building CASCADE;
CREATE VIEW results_dsra_sim6p8_cr2022.dsra_sim6p8_cr2022_rlz_1_damage_state_building AS 

-- 3.0 Earthquake Scenario Risk (DSRA)
-- 3.2 Building Performance
SELECT 
a."AssetID",
b.sauid AS "Sauid",

-- 3.2.1 Damage State - b0
CAST(CAST(ROUND(CAST(a."sD_None_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sD_None_b0",
CAST(CAST(ROUND(CAST(a."sD_None_b0" / b.number AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDr_None_b0",
CAST(CAST(ROUND(CAST(a."sD_None_stdv_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDsd_None_b0",

CAST(CAST(ROUND(CAST(a."sD_Slight_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sD_Slight_b0",
CAST(CAST(ROUND(CAST(a."sD_Slight_b0" / b.number AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDr_Slight_b0",
CAST(CAST(ROUND(CAST(a."sD_Slight_stdv_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDsd_Slight_b0",

CAST(CAST(ROUND(CAST(a."sD_Moderate_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sD_Moderate_b0",
CAST(CAST(ROUND(CAST(a."sD_Moderate_b0" / b.number AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDr_Moderate_b0",
CAST(CAST(ROUND(CAST(a."sD_Moderate_stdv_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDsd_Moderate_b0",

CAST(CAST(ROUND(CAST(a."sD_Extensive_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sD_Extensive_b0",
CAST(CAST(ROUND(CAST(a."sD_Extensive_b0" / b.number AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDr_Extensive_b0",
CAST(CAST(ROUND(CAST(a."sD_Extensive_stdv_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDsd_Extensive_b0",

CAST(CAST(ROUND(CAST(a."sD_Complete_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sD_Complete_b0",
CAST(CAST(ROUND(CAST(a."sD_Complete_b0" / b.number AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDr_Complete_b0",
CAST(CAST(ROUND(CAST(a."sD_Complete_stdv_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDsd_Complete_b0",

CAST(CAST(ROUND(CAST(a."sD_Collapse_b0" * b.number AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sD_Collapse_b0",
CAST(CAST(ROUND(CAST(a."sD_Collapse_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDr_Collapse_b0",
CAST(CAST(ROUND(CAST(a."sD_Complete_stdv_b0" * g.collapse_pc AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDsd_Collapse_b0",

-- 3.2.1 Damage State - r2
CAST(CAST(ROUND(CAST(a."sD_None_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sD_None_r2",
CAST(CAST(ROUND(CAST(a."sD_None_r2" / b.number AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDr_None_r2",
CAST(CAST(ROUND(CAST(a."sD_None_stdv_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDsd_None_r2",

CAST(CAST(ROUND(CAST(a."sD_Slight_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sD_Slight_r2",
CAST(CAST(ROUND(CAST(a."sD_Slight_r2" / b.number AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDr_Slight_r2",
CAST(CAST(ROUND(CAST(a."sD_Slight_stdv_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDsd_Slight_r2",

CAST(CAST(ROUND(CAST(a."sD_Moderate_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sD_Moderate_r2",
CAST(CAST(ROUND(CAST(a."sD_Moderate_r2" / b.number AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDr_Moderate_r2",
CAST(CAST(ROUND(CAST(a."sD_Moderate_stdv_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDsd_Moderate_r2",

CAST(CAST(ROUND(CAST(a."sD_Extensive_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sD_Extensive_r2",
CAST(CAST(ROUND(CAST(a."sD_Extensive_r2" / b.number AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDr_Extensive_r2",
CAST(CAST(ROUND(CAST(a."sD_Extensive_stdv_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDsd_Extensive_r2",

CAST(CAST(ROUND(CAST(a."sD_Complete_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sD_Complete_r2",
CAST(CAST(ROUND(CAST(a."sD_Complete_r2" / b.number AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDr_Complete_r2",
CAST(CAST(ROUND(CAST(a."sD_Complete_stdv_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDsd_Complete_r2",

CAST(CAST(ROUND(CAST(a."sD_Collapse_r2" * b.number AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sD_Collapse_r2",
CAST(CAST(ROUND(CAST(a."sD_Collapse_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDr_Collapse_r2",
CAST(CAST(ROUND(CAST(a."sD_Complete_stdv_r2" * g.collapse_pc AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sDsd_Collapse_r2",

b.geom AS "geom_point"

FROM dsra.dsra_sim6p8_cr2022_rlz_1 a
LEFT JOIN exposure.canada_exposure b ON a."AssetID" = b.id 
LEFT JOIN lut.retrofit_costs c ON b.eqbldgtype = c."Eq_BldgType"
LEFT JOIN vs30.vs30_bc_site_model_xref d ON a."AssetID" = d.id
LEFT JOIN gmf.gmfdata_sitemesh_sim6p8_cr2022_37_xref e ON b.id = e.id
LEFT JOIN ruptures.rupture_table f ON f.rupture_name = a."Rupture_Abbr"
LEFT JOIN lut.collapse_probability g ON b.eqbldgtype = g.eqbldgtype
LEFT JOIN census.census_2016_canada h ON b.sauid = h.sauidt;


-- create scenario risk building indicators
DROP VIEW IF EXISTS results_dsra_sim6p8_cr2022.dsra_sim6p8_cr2022_rlz_1_recovery_time_building CASCADE;
CREATE VIEW results_dsra_sim6p8_cr2022.dsra_sim6p8_cr2022_rlz_1_recovery_time_building AS 

-- 3.0 Earthquake Scenario Risk (DSRA)
-- 3.2 Building Performance
SELECT 
a."AssetID",
b.sauid AS "Sauid",

-- 3.2.1 Recovery Time - b0
CAST(CAST(ROUND(CAST(a."sC_Repair_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_Repair_b0",
CAST(CAST(ROUND(CAST(a."sC_Construxn_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_Construxn_b0",
CAST(CAST(ROUND(CAST(a."sC_Downtime_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_Downtime_b0",
CAST(CAST(ROUND(CAST(a."sC_DebrisBW_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DebrisBW_b0",
CAST(CAST(ROUND(CAST(a."sC_DebrisC_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DebrisCS_b0",

-- 3.2.1 Recovery Time - r2
CAST(CAST(ROUND(CAST(a."sC_Repair_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_Repair_r2",
CAST(CAST(ROUND(CAST(a."sC_Construxn_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_Construxn_r2",
CAST(CAST(ROUND(CAST(a."sC_Downtime_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_Downtime_r2",
CAST(CAST(ROUND(CAST(a."sC_DebrisBW_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DebrisBW_r2",
CAST(CAST(ROUND(CAST(a."sC_DebrisC_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DebrisCS_r2",

b.geom AS "geom_point"

FROM dsra.dsra_sim6p8_cr2022_rlz_1 a
LEFT JOIN exposure.canada_exposure b ON a."AssetID" = b.id 
LEFT JOIN lut.retrofit_costs c ON b.eqbldgtype = c."Eq_BldgType"
LEFT JOIN vs30.vs30_bc_site_model_xref d ON a."AssetID" = d.id
LEFT JOIN gmf.gmfdata_sitemesh_sim6p8_cr2022_37_xref e ON b.id = e.id
LEFT JOIN ruptures.rupture_table f ON f.rupture_name = a."Rupture_Abbr"
LEFT JOIN lut.collapse_probability g ON b.eqbldgtype = g.eqbldgtype
LEFT JOIN census.census_2016_canada h ON b.sauid = h.sauidt;

-- create schema for new scenario
CREATE SCHEMA IF NOT EXISTS results_dsra_sim6p8_cr2022;

-- create scenario risk building indicators
DROP VIEW IF EXISTS results_dsra_sim6p8_cr2022.dsra_sim6p8_cr2022_rlz_1_casualties_building CASCADE;
CREATE VIEW results_dsra_sim6p8_cr2022.dsra_sim6p8_cr2022_rlz_1_casualties_building AS 

-- 3.0 Earthquake Scenario Risk (DSRA)
-- 3.3 Affected People
SELECT 
a."AssetID",
b.sauid AS "Sauid",

-- 3.3.1 Casualties - b0
CAST(CAST(ROUND(CAST(a."sL_Fatalities_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sL_Fatality_b0",
CAST(CAST(ROUND(CAST(a."sL_Fatalities_stdv_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLsd_Fatality_b0",
CAST(CAST(ROUND(CAST(a."sC_CasDayL1_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasDayL1_b0",
CAST(CAST(ROUND(CAST(a."sC_CasDayL2_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasDayL2_b0",
CAST(CAST(ROUND(CAST(a."sC_CasDayL3_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasDayL3_b0",
CAST(CAST(ROUND(CAST(a."sC_CasDayL4_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasDayL4_b0",
CAST(CAST(ROUND(CAST(a."sC_CasNightL1_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasNightL1_b0",
CAST(CAST(ROUND(CAST(a."sC_CasNightL2_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasNightL2_b0",
CAST(CAST(ROUND(CAST(a."sC_CasNightL3_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasNightL3_b0",
CAST(CAST(ROUND(CAST(a."sC_CasNightL4_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasNightL4_b0",
CAST(CAST(ROUND(CAST(a."sC_CasTransitL1_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasTransitL1_b0",
CAST(CAST(ROUND(CAST(a."sC_CasTransitL2_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasTransitL2_b0",
CAST(CAST(ROUND(CAST(a."sC_CasTransitL3_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasTransitL3_b0",
CAST(CAST(ROUND(CAST(a."sC_CasTransitL4_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasTransitL4_b0",

-- 3.3.1 Casualties - r2
CAST(CAST(ROUND(CAST(a."sL_Fatalities_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sL_Fatality_r2",
CAST(CAST(ROUND(CAST(a."sL_Fatalities_stdv_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLsd_Fatality_r2",
CAST(CAST(ROUND(CAST(a."sC_CasDayL1_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasDayL1_r2",
CAST(CAST(ROUND(CAST(a."sC_CasDayL2_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasDayL2_r2",
CAST(CAST(ROUND(CAST(a."sC_CasDayL3_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasDayL3_r2",
CAST(CAST(ROUND(CAST(a."sC_CasDayL4_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasDayL4_r2",
CAST(CAST(ROUND(CAST(a."sC_CasNightL1_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasNightL1_r2",
CAST(CAST(ROUND(CAST(a."sC_CasNightL2_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasNightL2_r2",
CAST(CAST(ROUND(CAST(a."sC_CasNightL3_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasNightL3_r2",
CAST(CAST(ROUND(CAST(a."sC_CasNightL4_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasNightL4_r2",
CAST(CAST(ROUND(CAST(a."sC_CasTransitL1_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasTransitL1_r2",
CAST(CAST(ROUND(CAST(a."sC_CasTransitL2_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasTransitL2_r2",
CAST(CAST(ROUND(CAST(a."sC_CasTransitL3_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasTransitL3_r2",
CAST(CAST(ROUND(CAST(a."sC_CasTransitL4_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_CasTransitL4_r2",

b.geom AS "geom_point"

FROM dsra.dsra_sim6p8_cr2022_rlz_1 a
LEFT JOIN exposure.canada_exposure b ON a."AssetID" = b.id 
LEFT JOIN lut.retrofit_costs c ON b.eqbldgtype = c."Eq_BldgType"
LEFT JOIN vs30.vs30_bc_site_model_xref d ON a."AssetID" = d.id
LEFT JOIN gmf.gmfdata_sitemesh_sim6p8_cr2022_37_xref e ON b.id = e.id
LEFT JOIN ruptures.rupture_table f ON f.rupture_name = a."Rupture_Abbr"
LEFT JOIN lut.collapse_probability g ON b.eqbldgtype = g.eqbldgtype
LEFT JOIN census.census_2016_canada h ON b.sauid = h.sauidt;


DROP VIEW IF EXISTS results_dsra_sim6p8_cr2022.dsra_sim6p8_cr2022_rlz_1_social_disruption_building CASCADE;
CREATE VIEW results_dsra_sim6p8_cr2022.dsra_sim6p8_cr2022_rlz_1_social_disruption_building AS 

-- 3.0 Earthquake Scenario Risk (DSRA)
-- 3.3 Affected People
SELECT 
a."AssetID",
b.sauid AS "Sauid",

-- 3.3.2 Social Disruption - b0
-- sC_Shelter -- calculated at sauid level only
CAST(CAST(ROUND(CAST(a."sC_DisplRes_3_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplRes3_b0",
CAST(CAST(ROUND(CAST(a."sC_DisplRes_30_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplRes30_b0",
CAST(CAST(ROUND(CAST(a."sC_DisplRes_90_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplRes90_b0",
CAST(CAST(ROUND(CAST(a."sC_DisplRes_180_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplRes180_b0",
CAST(CAST(ROUND(CAST(a."sC_DisplRes_360_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplRes360_b0",

CAST(CAST(ROUND(CAST(COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_b0" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_b0" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / 
NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplHshld_b0",

CAST(CAST(ROUND(CAST((CASE WHEN a."sC_Downtime_b0" > 3 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_b0" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_b0" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / 
NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplHshld3_b0",

CAST(CAST(ROUND(CAST((CASE WHEN a."sC_Downtime_b0" > 30 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_b0" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_b0" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / 
NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplHshld30_b0",

CAST(CAST(ROUND(CAST((CASE WHEN a."sC_Downtime_b0" > 90 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_b0" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_b0" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / 
NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplHshld90_b0",

CAST(CAST(ROUND(CAST((CASE WHEN a."sC_Downtime_b0" > 180 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_b0" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_b0" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / 
NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplHshld180_b0",

CAST(CAST(ROUND(CAST((CASE WHEN a."sC_Downtime_b0" > 360 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_b0" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_b0" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_b0" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_b0" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / 
NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplHshld360_b0",

CAST(CAST(ROUND(CAST(a."sC_DisrupEmpl_30_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisrupEmpl_30_b0",
CAST(CAST(ROUND(CAST(a."sC_DisrupEmpl_90_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisrupEmpl_90_b0",
CAST(CAST(ROUND(CAST(a."sC_DisrupEmpl_180_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisrupEmpl_180_b0",
CAST(CAST(ROUND(CAST(a."sC_DisrupEmpl_360_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisrupEmpl_360_b0",

-- 3.3.2 Social Disruption - r2
-- sC_Shelter -- calculated at sauid level only
CAST(CAST(ROUND(CAST(a."sC_DisplRes_3_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplRes3_r2",
CAST(CAST(ROUND(CAST(a."sC_DisplRes_30_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplRes30_r2",
CAST(CAST(ROUND(CAST(a."sC_DisplRes_90_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplRes90_r2",
CAST(CAST(ROUND(CAST(a."sC_DisplRes_360_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplRes360_r2",

CAST(CAST(ROUND(CAST(COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_r2" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_r2" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / 
NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplHshld_r2",

CAST(CAST(ROUND(CAST((CASE WHEN a."sC_Downtime_r2" > 3 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_r2" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_r2" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / 
NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplHshld3_r2",

CAST(CAST(ROUND(CAST((CASE WHEN a."sC_Downtime_r2" > 30 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_r2" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_r2" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / 
NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplHshld30_r2",

CAST(CAST(ROUND(CAST((CASE WHEN a."sC_Downtime_r2" > 90 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_r2" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_r2" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / 
NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplHshld90_r2",

CAST(CAST(ROUND(CAST((CASE WHEN a."sC_Downtime_r2" > 180 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_r2" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_r2" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / 
NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplHshld180_r2",

CAST(CAST(ROUND(CAST((CASE WHEN a."sC_Downtime_r2" > 360 THEN (COALESCE((((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) * 
((0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-LD' THEN a."sD_Complete_r2" / b.number ELSE 0 END)))) + 
((CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) *
((0 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Moderate_r2" / b.number ELSE 0 END)) + 
(0.9 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Extensive_r2" / b.number ELSE 0 END)) + 
(1 * (CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN a."sD_Complete_r2" / b.number ELSE 0 END))))) * 
((CASE WHEN b.genocc ='Residential-LD' OR b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END) / 
NULLIF((CASE WHEN b.genocc ='Residential-LD' THEN b.night/h.people_du ELSE 0 END) + 
(CASE WHEN b.genocc ='Residential-MD' OR b.genocc ='Residential-HD' THEN b.night/h.people_du ELSE 0 END),0)),0)) ELSE 0 END) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisplHshld360_r2",

CAST(CAST(ROUND(CAST(a."sC_DisrupEmpl_30_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisrupEmpl_30_r2",
CAST(CAST(ROUND(CAST(a."sC_DisrupEmpl_90_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisrupEmpl_90_r2",
CAST(CAST(ROUND(CAST(a."sC_DisrupEmpl_180_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisrupEmpl_180_r2",
CAST(CAST(ROUND(CAST(a."sC_DisrupEmpl_360_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sC_DisrupEmpl_360_r2",

b.geom AS "geom_point"

FROM dsra.dsra_sim6p8_cr2022_rlz_1 a
LEFT JOIN exposure.canada_exposure b ON a."AssetID" = b.id 
LEFT JOIN lut.retrofit_costs c ON b.eqbldgtype = c."Eq_BldgType"
LEFT JOIN vs30.vs30_bc_site_model_xref d ON a."AssetID" = d.id
LEFT JOIN gmf.gmfdata_sitemesh_sim6p8_cr2022_37_xref e ON b.id = e.id
LEFT JOIN ruptures.rupture_table f ON f.rupture_name = a."Rupture_Abbr"
LEFT JOIN lut.collapse_probability g ON b.eqbldgtype = g.eqbldgtype
LEFT JOIN census.census_2016_canada h ON b.sauid = h.sauidt;

-- create schema for new scenario
CREATE SCHEMA IF NOT EXISTS results_dsra_sim6p8_cr2022;

-- create scenario risk building indicators
DROP VIEW IF EXISTS results_dsra_sim6p8_cr2022.dsra_sim6p8_cr2022_rlz_1_economic_loss_building CASCADE;
CREATE VIEW results_dsra_sim6p8_cr2022.dsra_sim6p8_cr2022_rlz_1_economic_loss_building AS 

-- 3.0 Earthquake Scenario Risk (DSRA)
-- 3.4 Economic Security
SELECT 
a."AssetID",
b.sauid AS "Sauid",

-- 3.4.1 Economic Loss - b0
CAST(CAST(ROUND(CAST(a."sL_Str_b0" + a."sL_NStr_b0" + a."sL_Cont_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sL_Asset_b0",
CAST(CAST(ROUND(CAST(a."sL_Str_b0" + a."sL_NStr_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sL_Bldg_b0",
CAST(CAST(ROUND(CAST(COALESCE(((a."sL_Str_b0" + a."sL_NStr_b0")/(b.number))/NULLIF(((a."sL_Str_b0" + a."sL_NStr_b0" + a."sL_Cont_b0")/(b.number)),0),0) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLr_Bldg_b0",
CAST(CAST(ROUND(CAST((((a."sL_Str_b0" + a."sL_NStr_b0" + a."sL_Cont_b0") - (a."sL_Str_r2" + a."sL_NStr_r2" + a."sL_Cont_r2"))/(c."CAD_RetrofitCost_Bldg")) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLr2_BCR_b0",
CAST(CAST(ROUND(CAST((((a."sL_Str_b0" + a."sL_NStr_b0" + a."sL_Cont_b0") - (a."sL_Str_r2" + a."sL_NStr_r2" + a."sL_Cont_r2")) * ((EXP(-0.03*100)/0.03)/(c."CAD_RetrofitCost_Bldg"))) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "SLr2_RoI",

CAST(CAST(ROUND(CAST(a."sL_Str_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sL_Str_b0",
CAST(CAST(ROUND(CAST(a."sL_Str_stdv_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLsd_Str_b0",

CAST(CAST(ROUND(CAST(a."sL_NStr_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sL_NStr_b0",
CAST(CAST(ROUND(CAST(a."sL_NStr_stdv_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLsd_NStr_b0",

CAST(CAST(ROUND(CAST(a."sL_Cont_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sL_Cont_b0",
CAST(CAST(ROUND(CAST(a."sL_Cont_stdv_b0" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLsd_Cont_b0",

-- 3.4.1 Economic Loss - r2
CAST(CAST(ROUND(CAST(a."sL_Str_r2" + a."sL_NStr_r2" + a."sL_Cont_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sL_Asset_r2",
CAST(CAST(ROUND(CAST(a."sL_Str_r2" + a."sL_NStr_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sL_Bldg_r2",
CAST(CAST(ROUND(CAST(COALESCE(((a."sL_Str_r2" + a."sL_NStr_r2")/(b.number))/NULLIF(((a."sL_Str_r2" + a."sL_NStr_r2" + a."sL_Cont_r2")/(b.number)),0),0) AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLr_Bldg_r2",

CAST(CAST(ROUND(CAST(a."sL_Str_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sL_Str_r2",
CAST(CAST(ROUND(CAST(a."sL_Str_stdv_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLsd_Str_r2",

CAST(CAST(ROUND(CAST(a."sL_NStr_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sL_NStr_r2",
CAST(CAST(ROUND(CAST(a."sL_NStr_stdv_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLsd_NStr_r2",

CAST(CAST(ROUND(CAST(a."sL_Cont_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sL_Cont_r2",
CAST(CAST(ROUND(CAST(a."sL_Cont_stdv_r2" AS NUMERIC),6) AS FLOAT) AS NUMERIC) AS "sLsd_Cont_r2",

b.geom AS "geom_point"

FROM dsra.dsra_sim6p8_cr2022_rlz_1 a
LEFT JOIN exposure.canada_exposure b ON a."AssetID" = b.id 
LEFT JOIN lut.retrofit_costs c ON b.eqbldgtype = c."Eq_BldgType"
LEFT JOIN vs30.vs30_bc_site_model_xref d ON a."AssetID" = d.id
LEFT JOIN gmf.gmfdata_sitemesh_sim6p8_cr2022_37_xref e ON b.id = e.id
LEFT JOIN ruptures.rupture_table f ON f.rupture_name = a."Rupture_Abbr"
LEFT JOIN lut.collapse_probability g ON b.eqbldgtype = g.eqbldgtype
LEFT JOIN census.census_2016_canada h ON b.sauid = h.sauidt;