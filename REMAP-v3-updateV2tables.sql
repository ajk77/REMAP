/*
REMAP-v3-update_v2Tables.sql
created by King

NAVIGATION: 
	ALL are COVID_PHI
	/ v2EnrolledPerson /
	/ v2EnrolledIcuAdmitsM /
	/ v2EnrolledIcuAdmitsS /
	/ v2StateHypoxiaAtEnrollM /
	/ v2StateHypoxiaAtEnrollS /
	/ v2HypoxiaVarM /
	/ v2HypoxiaVarS /
	/ v2StudyDayM /
	/ v2StudyDayS /
	/ v2VasoInstanceM /
	/ v2VasoInstanceS /
	/ v2HFNCInstanceM /
	/ v2HFNCInstanceS /
	#/ v2RelaxedHFNCInstanceM /  ## removed and replaced with v2SupplementalOxygenInstanceM on 1/25/21 
	#/ v2RelaxedHFNCInstanceS /  ## " " 
	/ v2SupplementalOxygenInstanceM /
	/ v2SupplementalOxygenInstanceS /
	/ v2ECMOInstanceM /
	/ v2ECMOInstanceS /
	/ v2NivInstancesM /
	/ v2NivInstancesS /
	/ v2IVInstancesM /
	/ v2IVInstancesS /
	/ v2RRTInstanceM /
	/ v2RRTInstanceS /
	/ v2SofaInstancesM /
	/ v2SofaInstancesS /
	/ v2HourlyFiO2MeasurementsM /
	/ v2HourlyFiO2MeasurementsS /
	/ v2testDailyCRFM /
	/ v2testDailyCRFS /
	/ v2ApacheeTemperaturesS / # Apachee tables are not v3 optimized 
	/ v2ApacheeTemperatureSitesS /
	/ v2ApacheeBloodPressuresS /
	/ v2ApacheeCO2S /
	/ v2ApacheeOxygenationMeasurementsS /
	/ v2ApacheeOxygenationDevicesS /
	/ v2ApacheeOxygenationS /
	
*/

	### Create v2EnrolledPerson ###
	DROP TABLE COVID_PHI.v2EnrolledPerson; 
	CREATE TABLE COVID_PHI.v2EnrolledPerson
		SELECT P.PERSON_ID, P.MRN, I.ENCNTR_ID, I.FIN, P.screendate_utc, P.STUDYPATIENTID, 
			M.randomized_utc AS RandomizedModerate_utc, S.randomized_utc AS RandomizedSevere_utc, 
			L.StartOfHospitalization_utc, L.EndOfHospitalization_utc, P.REGIMEN, CURRENT_TIMESTAMP AS last_update 
		FROM REMAP.v3Participant P
		JOIN REMAP.v3IdMap I ON P.STUDYPATIENTID = I.STUDYPATIENTID
		LEFT JOIN REMAP.v3RandomizedModerate M ON P.STUDYPATIENTID = M.STUDYPATIENTID
		LEFT JOIN REMAP.v3RandomizedSevere S ON P.STUDYPATIENTID = S.STUDYPATIENTID
		LEFT JOIN (SELECT STUDYPATIENTID, MIN(beg_utc) AS StartOfHospitalization_utc, MAX(end_utc) AS EndOfHospitalization_utc
			FROM REMAP.v3LocOrder GROUP BY STUDYPATIENTID) AS L ON P.STUDYPATIENTID = L.STUDYPATIENTID
		ORDER BY STUDYPATIENTID, ENCNTR_ID
	; 
	SELECT * FROM COVID_PHI.v2EnrolledPerson;
	

	### Create v2EnrolledIcuAdmitsM ###
	DROP TABLE COVID_PHI.v2EnrolledIcuAdmitsM;
	CREATE TABLE COVID_PHI.v2EnrolledIcuAdmitsM
		SELECT DISTINCT S.upk AS stay_id, S.StudyPatientID, P.PERSON_ID, I.encntr_id, I.fin, S.stay_count,
			S.beg_utc AS start_dt_utc, S.end_utc AS end_dt_utc, 
			TIMESTAMPDIFF(HOUR, R.randomized_utc, S.beg_utc) AS start_hr,   
			TIMESTAMPDIFF(HOUR, R.randomized_utc, S.end_utc) AS end_hr, 
			S.includes_stepdownUnit AS includes_stepdown,
			CURRENT_TIMESTAMP AS last_update 
		FROM REMAP.v3IcuStay S
		JOIN REMAP.v3RandomizedModerate R ON S.STUDYPATIENTID = R.STUDYPATIENTID
		JOIN REMAP.v3LocOrder L ON S.STUDYPATIENTID = L.STUDYPATIENTID AND S.loc_start = L.loc_order
		JOIN REMAP.v3Participant P ON S.STUDYPATIENTID = P.STUDYPATIENTID
		JOIN REMAP.v3IdMap I ON S.STUDYPATIENTID = I.STUDYPATIENTID AND L.encntr_id = I.ENCNTR_ID
		ORDER BY S.STUDYPATIENTID, S.beg_utc
	;


	### Create v2EnrolledIcuAdmitsS ###
	DROP TABLE COVID_PHI.v2EnrolledIcuAdmitsS;
	CREATE TABLE COVID_PHI.v2EnrolledIcuAdmitsS
		SELECT DISTINCT S.upk AS stay_id, S.StudyPatientID, P.PERSON_ID, I.encntr_id, I.fin, S.stay_count,
			S.beg_utc AS start_dt_utc, S.end_utc AS end_dt_utc, 
			TIMESTAMPDIFF(HOUR, R.randomized_utc, S.beg_utc) AS start_hr,   
			TIMESTAMPDIFF(HOUR, R.randomized_utc, S.end_utc) AS end_hr, 
			S.includes_stepdownUnit AS includes_stepdown,
			CURRENT_TIMESTAMP AS last_update 
		FROM REMAP.v3IcuStay S
		JOIN REMAP.v3RandomizedSevere R ON S.STUDYPATIENTID = R.STUDYPATIENTID
		JOIN REMAP.v3LocOrder L ON S.STUDYPATIENTID = L.STUDYPATIENTID AND S.loc_start = L.loc_order
		JOIN REMAP.v3Participant P ON S.STUDYPATIENTID = P.STUDYPATIENTID
		JOIN REMAP.v3IdMap I ON S.STUDYPATIENTID = I.STUDYPATIENTID AND L.encntr_id = I.ENCNTR_ID
		ORDER BY S.STUDYPATIENTID, S.beg_utc
	;

	### Create v2StateHypoxiaAtEnrollM ###
	DROP TABLE COVID_PHI.v2StateHypoxiaAtEnrollM;
	CREATE TABLE COVID_PHI.v2StateHypoxiaAtEnrollM
		SELECT 
			R.StudyPatientID, R.randomized_utc AS RandomizationTime_utc, ifnull(H.StateHypoxia, 1) AS hypoxia_state,
			ifnull(H.onInvasiveVent, 0) AS on_mechanical_breathing_support, if(H.PaO2_utc IS NOT NULL, 1, 0) AS ABG_avaliable,
			MAX(H.PEEP_float) AS PEEP_value, H.PEEP_utc AS PEEP_dt_utc,
			MIN(H.PF_ratio) AS PF_ratio,
			MAX(H.PaO2_float) AS PaO2_value, H.PaO2_utc AS PaO2_dt_utc,
			MAX(H.FiO2_float) AS FiO2_value, H.FiO2_utc AS FiO2_dt_utc,
			CURRENT_TIMESTAMP AS last_update 
		FROM REMAP.v3RandomizedModerate R
		LEFT JOIN REMAP.v3CalculatedStateHypoxiaAtEnroll H ON R.STUDYPATIENTID = H.STUDYPATIENTID
		WHERE RandomizationType = 'Moderate' OR RandomizationType IS NULL 
		GROUP BY R.StudyPatientID, R.randomized_utc, H.StateHypoxia, H.onInvasiveVent,
			H.PEEP_utc,H.PaO2_utc, H.FiO2_utc  # grouping is needed b/c multiple values sometimes occur at the same timestamp
		ORDER BY R.StudyPatientID
	;
	
	### Create v2StateHypoxiaAtEnrollS ###
	DROP TABLE COVID_PHI.v2StateHypoxiaAtEnrollS;
	CREATE TABLE COVID_PHI.v2StateHypoxiaAtEnrollS
		SELECT 
			R.StudyPatientID, R.randomized_utc AS RandomizationTime_utc, ifnull(H.StateHypoxia, 1) AS hypoxia_state,
			ifnull(H.onInvasiveVent, 0) AS on_mechanical_breathing_support, if(H.PaO2_utc IS NOT NULL, 1, 0) AS ABG_avaliable,
			MAX(H.PEEP_float) AS PEEP_value, H.PEEP_utc AS PEEP_dt_utc,
			MIN(H.PF_ratio) AS PF_ratio,
			MAX(H.PaO2_float) AS PaO2_value, H.PaO2_utc AS PaO2_dt_utc,
			MAX(H.FiO2_float) AS FiO2_value, H.FiO2_utc AS FiO2_dt_utc,
			CURRENT_TIMESTAMP AS last_update 
		FROM REMAP.v3RandomizedSevere R
		LEFT JOIN REMAP.v3CalculatedStateHypoxiaAtEnroll H ON R.STUDYPATIENTID = H.STUDYPATIENTID
		WHERE RandomizationType = 'Severe' OR RandomizationType IS NULL
		GROUP BY R.StudyPatientID, R.randomized_utc, H.StateHypoxia, H.onInvasiveVent,
			H.PEEP_utc, H.PaO2_utc, H.FiO2_utc  # grouping is needed b/c multiple values sometimes occur at the same timestamp
		ORDER BY R.StudyPatientID
	;
	


	### Create v2HypoxiaVarM and v2HypoxiaVarS ###
	CREATE TABLE REMAP.v3tempHypoxiaVar
	SELECT 
		StudyPatientID, NULL AS PaO2_value, NULL AS PaO2_dt_utc, 
		FiO2_float AS FiO2_value, FiO2_utc AS FiO2_dt_utc,	NULL AS PF_ratio,
		PEEP_float AS PEEP_value, PEEP_utc AS PEEP_dt_utc, 
		'PEEP & FiO2 pair' AS row_type, CURRENT_TIMESTAMP AS last_update 
	FROM REMAP.v3CalculatedPEEPjoinFiO2
	UNION
	SELECT 
		StudyPatientID, PaO2_float AS PaO2_value, PaO2_utc AS PaO2_dt_utc,
		FiO2_float AS FiO2_value, FiO2_utc AS FiO2_dt_utc, PF_ratio,
		PEEP_float AS PEEP_value, PEEP_utc as PEEP_dt_utc,
		'P:F instance' AS row_type, CURRENT_TIMESTAMP AS last_update
	FROM REMAP.v3CalculatedPFratio
	;
	DROP TABLE COVID_PHI.v2HypoxiaVarM;
	CREATE TABLE COVID_PHI.v2HypoxiaVarM
		SELECT * FROM REMAP.v3tempHypoxiaVar
		WHERE studypatientid IN (SELECT studypatientid FROM REMAP.v3RandomizedModerate)
		ORDER BY studypatientid, FiO2_dt_utc
	;
	DROP TABLE COVID_PHI.v2HypoxiaVarS;
	CREATE TABLE COVID_PHI.v2HypoxiaVarS
		SELECT * FROM REMAP.v3tempHypoxiaVar
		WHERE studypatientid IN (SELECT studypatientid FROM REMAP.v3RandomizedSevere)
		ORDER BY studypatientid, FiO2_dt_utc
	;
	DROP TABLE REMAP.v3tempHypoxiaVar;

	### v2StudyDayM ###
	DROP TABLE COVID_PHI.v2StudyDayM;
	CREATE TABLE COVID_PHI.v2StudyDayM
		SELECT DISTINCT S.StudyPatientId, R.randomized_utc AS RandomizationTime_utc, S.study_day,
			S.day_date_local, S.day_start_utc, S.day_end_utc, CURRENT_TIMESTAMP as last_update, S.RandomizationType
		FROM REMAP.v3StudyDay S 
		JOIN REMAP.v3RandomizedModerate R 
		ON S.StudyPatientId = R.StudyPatientId
		WHERE RandomizationType = 'Moderate'
		ORDER BY S.StudyPatientID, STUDY_DAY DESC
	;
	
	### v2StudyDayS ###
	DROP TABLE COVID_PHI.v2StudyDayS;
	CREATE TABLE COVID_PHI.v2StudyDayS
		SELECT DISTINCT S.StudyPatientId, R.randomized_utc AS RandomizationTime_utc, S.study_day,
			S.day_date_local, S.day_start_utc, S.day_end_utc, CURRENT_TIMESTAMP as last_update, S.RandomizationType
		FROM REMAP.v3StudyDay S 
		JOIN REMAP.v3RandomizedSevere R 
		ON S.StudyPatientId = R.StudyPatientId
		WHERE RandomizationType = 'Severe'
		ORDER BY S.StudyPatientID, STUDY_DAY DESC 
	;

	### v2VasoInstanceM ###
	DROP TABLE COVID_PHI.v2VasoInstancesM;
	CREATE TABLE COVID_PHI.v2VasoInstancesM AS 	
	SELECT DISTINCT
		EIA.fin, 
		EIA.stay_count, 
		SD.StudyPatientId, SD.study_day,
		SD.RandomizationTime_utc,
		vaso_dt_utc,
		SD.last_update, SD.RandomizationType,
		CV.display, admin_dosage, CVU.display AS units, CVR.DISPLAY AS route
	FROM 
		(SELECT 	M.encntr_id, O.event_utc AS vaso_dt_utc, M.event_cd, M.admin_dosage, M.dosage_unit_cd, M.admin_route_cd
		FROM REMAP.v3OrganSupportInstance O
		JOIN CT_DATA.MAR_AD M ON O.event_id = M.EVENT_ID 
		WHERE support_type = 'Vasopressor'
		) AS vaso
		JOIN CT_DATA.CODE_VALUE CV ON (vaso.event_cd = CV.code_value) 
		JOIN CT_DATA.CODE_VALUE CVU ON (vaso.dosage_unit_cd = CVU.code_value) 
		JOIN CT_DATA.CODE_VALUE CVR ON (vaso.admin_route_cd = CVR.code_value)
		JOIN COVID_PHI.v2EnrolledIcuAdmitsM EIA ON (vaso.encntr_id = EIA.encntr_id)
		JOIN COVID_PHI.v2StudyDayM SD ON (EIA.StudyPatientId = SD.StudyPatientId AND vaso_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
	WHERE 
		vaso_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)			
	; 
		
	### v2VasoInstancesS ###
	DROP TABLE COVID_PHI.v2VasoInstancesS;
	CREATE TABLE COVID_PHI.v2VasoInstancesS AS 		
	SELECT DISTINCT
		EIA.fin, 
		EIA.stay_count, 
		SD.StudyPatientId, SD.study_day,
		SD.RandomizationTime_utc,
		vaso_dt_utc,
		SD.last_update, SD.RandomizationType,
		CV.display, admin_dosage, CVU.display AS units, CVR.DISPLAY AS route
	FROM 
		(SELECT 	M.encntr_id, O.event_utc AS vaso_dt_utc, M.event_cd, M.admin_dosage, M.dosage_unit_cd, M.admin_route_cd
		FROM REMAP.v3OrganSupportInstance O
		JOIN CT_DATA.MAR_AD M ON O.event_id = M.EVENT_ID 
		WHERE support_type = 'Vasopressor'
		) AS vaso
		JOIN CT_DATA.CODE_VALUE CV ON (vaso.event_cd = CV.code_value) 
		JOIN CT_DATA.CODE_VALUE CVU ON (vaso.dosage_unit_cd = CVU.code_value) 
		JOIN CT_DATA.CODE_VALUE CVR ON (vaso.admin_route_cd = CVR.code_value)
		JOIN COVID_PHI.v2EnrolledIcuAdmitsS EIA ON (vaso.encntr_id = EIA.encntr_id)
		JOIN COVID_PHI.v2StudyDayS SD ON (EIA.StudyPatientId = SD.StudyPatientId AND vaso_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
	WHERE 
		vaso_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)				
	;
		
	### v2HFNCInstanceM ###
	DROP TABLE COVID_PHI.v2HFNCInstancesM;
	CREATE TABLE COVID_PHI.v2HFNCInstancesM AS 
		SELECT DISTINCT
			EIA.fin, EIA.stay_count, 
			SD.StudyPatientId, SD.study_day,
			SD.RandomizationTime_utc, 
			hfnc_dt_utc, CURRENT_TIMESTAMP as last_update, SD.RandomizationType
		FROM
			(SELECT 	P.encntr_id, O.event_utc AS hfnc_dt_utc, O.StudyPatientID
			FROM REMAP.v3OrganSupportInstance O
			JOIN CT_DATA.CE_PHYSIO P ON O.event_id = P.EVENT_ID 
			WHERE support_type = 'HFNC'
			) AS device
			JOIN COVID_PHI.v2EnrolledIcuAdmitsM EIA ON (device.encntr_id = EIA.encntr_id)
			JOIN COVID_PHI.v2StudyDayM SD ON (EIA.StudyPatientId = SD.StudyPatientId 
				AND hfnc_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		WHERE 
			hfnc_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)
		;
			
	### v2HFNCInstanceS ###
	DROP TABLE COVID_PHI.v2HFNCInstancesS;
	CREATE TABLE COVID_PHI.v2HFNCInstancesS AS 
		SELECT DISTINCT
			EIA.fin, EIA.stay_count, 
			SD.StudyPatientId, SD.study_day,
			SD.RandomizationTime_utc, 
			hfnc_dt_utc, CURRENT_TIMESTAMP as last_update, SD.RandomizationType
		FROM
			(SELECT 	P.encntr_id, O.event_utc AS hfnc_dt_utc, O.StudyPatientID
			FROM REMAP.v3OrganSupportInstance O
			JOIN CT_DATA.CE_PHYSIO P ON O.event_id = P.EVENT_ID 
			WHERE support_type = 'HFNC'
			) AS device
			JOIN COVID_PHI.v2EnrolledIcuAdmitsS EIA ON (device.encntr_id = EIA.encntr_id)
			JOIN COVID_PHI.v2StudyDayS SD ON (EIA.StudyPatientId = SD.StudyPatientId 
				AND hfnc_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		WHERE 
			hfnc_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)
	;
	
	### v2SupplementalOxygenInstanceM ###
	DROP TABLE COVID_PHI.v2SupplementalOxygenInstanceM;
	CREATE TABLE COVID_PHI.v2SupplementalOxygenInstanceM AS 
		SELECT DISTINCT
			EIA.fin, EIA.stay_count, 
			SD.StudyPatientId, SD.study_day,
			SD.RandomizationTime_utc, 
			O2_dt_utc, CURRENT_TIMESTAMP as last_update, SD.RandomizationType
		FROM
			(SELECT 	P.encntr_id, SO.event_utc AS O2_dt_utc, SO.StudyPatientID
			FROM REMAP.v3SupplementalOxygenInstance SO
			JOIN CT_DATA.CE_PHYSIO P ON SO.event_id = P.EVENT_ID 
			) AS device
			JOIN COVID_PHI.v2EnrolledIcuAdmitsM EIA ON (device.encntr_id = EIA.encntr_id)
			JOIN COVID_PHI.v2StudyDayM SD ON (EIA.StudyPatientId = SD.StudyPatientId 
				AND O2_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		WHERE 
			O2_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)
		;
		
	
	### v2SupplementalOxygenInstanceS ###
	DROP TABLE COVID_PHI.v2SupplementalOxygenInstanceS;
	CREATE TABLE COVID_PHI.v2SupplementalOxygenInstanceS AS 
	SELECT DISTINCT
			EIA.fin, EIA.stay_count, 
			SD.StudyPatientId, SD.study_day,
			SD.RandomizationTime_utc, 
			O2_dt_utc, CURRENT_TIMESTAMP as last_update, SD.RandomizationType
		FROM
			(SELECT 	P.encntr_id, SO.event_utc AS O2_dt_utc, SO.StudyPatientID
			FROM REMAP.v3SupplementalOxygenInstance SO
			JOIN CT_DATA.CE_PHYSIO P ON SO.event_id = P.EVENT_ID 
			) AS device
			JOIN COVID_PHI.v2EnrolledIcuAdmitsS EIA ON (device.encntr_id = EIA.encntr_id)
			JOIN COVID_PHI.v2StudyDayS SD ON (EIA.StudyPatientId = SD.StudyPatientId 
				AND O2_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		WHERE 
			O2_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)
		;
	
	
	### v2ECMOInstanceM ###
	DROP TABLE COVID_PHI.v2ECMOInstancesM;
	CREATE TABLE COVID_PHI.v2ECMOInstancesM AS
		SELECT DISTINCT
			EIA.fin, 
			EIA.stay_count, 
			SD.StudyPatientId, SD.study_day,
			SD.RandomizationTime_utc,
			ecmo_dt_utc,
			SD.last_update, SD.RandomizationType
		FROM 
			(SELECT 	P.encntr_id, O.event_utc AS ecmo_dt_utc, O.StudyPatientID
			FROM REMAP.v3OrganSupportInstance O
			JOIN CT_DATA.CE_PHYSIO P ON O.event_id = P.EVENT_ID 
			WHERE support_type = 'ECMO'
			) AS CEP
			JOIN COVID_PHI.v2EnrolledIcuAdmitsM EIA ON (CEP.encntr_id = EIA.encntr_id)
			JOIN COVID_PHI.v2StudyDayM SD ON (EIA.StudyPatientId = SD.StudyPatientId AND ecmo_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		WHERE 
			ecmo_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)	
	;

	### v2ECMOInstanceS ###
	DROP TABLE COVID_PHI.v2ECMOInstancesS;
	CREATE TABLE COVID_PHI.v2ECMOInstancesS AS
		SELECT DISTINCT
			EIA.fin, 
			EIA.stay_count, 
			SD.StudyPatientId, SD.study_day,
			SD.RandomizationTime_utc,
			ecmo_dt_utc,
			SD.last_update, SD.RandomizationType
		FROM 
			(SELECT 	P.encntr_id, O.event_utc AS ecmo_dt_utc, O.StudyPatientID
			FROM REMAP.v3OrganSupportInstance O
			JOIN CT_DATA.CE_PHYSIO P ON O.event_id = P.EVENT_ID 
			WHERE support_type = 'ECMO'
			) AS CEP
			JOIN COVID_PHI.v2EnrolledIcuAdmitsS EIA ON (CEP.encntr_id = EIA.encntr_id)
			JOIN COVID_PHI.v2StudyDayS SD ON (EIA.StudyPatientId = SD.StudyPatientId AND ecmo_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		WHERE 
			ecmo_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)			
	;
	
	### v2NivInstancesM ###
	DROP TABLE COVID_PHI.v2NivInstancesM;
	CREATE TABLE COVID_PHI.v2NivInstancesM AS
	WITH NIV_instances AS (
		SELECT DISTINCT
			EIA.fin, 
			EIA.stay_count, 
			SD.StudyPatientID, SD.study_day,
			SD.RandomizationTime_utc,
			mechSupport_dt_utc,
			DATEDIFF(REMAP.to_local(mechSupport_dt_utc), REMAP.to_local(SD.RandomizationTime_utc))*2 
				+ (TIME(REMAP.to_local(mechSupport_dt_utc))>='12:00:00') AS half_days_since_randomization,
			SD.last_update, SD.RandomizationType, documented_source
		FROM 
			(SELECT P.encntr_id, O.event_utc AS mechSupport_dt_utc, O.StudyPatientID, O.documented_source
			FROM REMAP.v3OrganSupportInstance O 
			JOIN CT_DATA.CE_PHYSIO P ON O.event_id = P.EVENT_ID
			WHERE support_type = 'NIV'
			) AS CEP
			JOIN COVID_PHI.v2EnrolledIcuAdmitsM EIA ON (CEP.encntr_id = EIA.encntr_id)
			JOIN COVID_PHI.v2StudyDayM SD ON (EIA.StudyPatientId = SD.StudyPatientId AND mechSupport_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		WHERE 
			mechSupport_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)			
	)
	SELECT N1.*, N2.half_days_since_randomization AS linked_half_day
	FROM NIV_instances N1
	JOIN NIV_instances N2 ON N1.StudyPatientID = N2.StudyPatientID
	WHERE N1.half_days_since_randomization = N2.half_days_since_randomization + 1
	UNION 
	SELECT N1.*, N2.half_days_since_randomization AS linked_half_day
	FROM NIV_instances N1
	JOIN NIV_instances N2 ON N1.StudyPatientID = N2.StudyPatientID
	WHERE N1.half_days_since_randomization = N2.half_days_since_randomization - 1
	;
		
	### v2NivInstancesS ###
	DROP TABLE COVID_PHI.v2NivInstancesS;
	CREATE TABLE COVID_PHI.v2NivInstancesS AS
	WITH NIV_instances AS (
		SELECT DISTINCT
			EIA.fin, 
			EIA.stay_count, 
			SD.StudyPatientID, SD.study_day,
			SD.RandomizationTime_utc,
			mechSupport_dt_utc,
			DATEDIFF(REMAP.to_local(mechSupport_dt_utc), REMAP.to_local(SD.RandomizationTime_utc))*2 
				+ (TIME(REMAP.to_local(mechSupport_dt_utc))>='12:00:00') AS half_days_since_randomization,
			SD.last_update, SD.RandomizationType, documented_source
		FROM 
			(SELECT P.encntr_id, O.event_utc AS mechSupport_dt_utc, O.StudyPatientID, O.documented_source
			FROM REMAP.v3OrganSupportInstance O 
			JOIN CT_DATA.CE_PHYSIO P ON O.event_id = P.EVENT_ID
			WHERE support_type = 'NIV'
		) AS CEP
			JOIN COVID_PHI.v2EnrolledIcuAdmitsS EIA ON (CEP.encntr_id = EIA.encntr_id)
			JOIN COVID_PHI.v2StudyDayS SD ON (EIA.StudyPatientId = SD.StudyPatientId AND mechSupport_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		WHERE 
			mechSupport_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)			
	)
	SELECT N1.*, N2.half_days_since_randomization AS linked_half_day
	FROM NIV_instances N1
	JOIN NIV_instances N2 ON N1.StudyPatientID = N2.StudyPatientID
	WHERE N1.half_days_since_randomization = N2.half_days_since_randomization + 1
	UNION 
	SELECT N1.*, N2.half_days_since_randomization AS linked_half_day
	FROM NIV_instances N1
	JOIN NIV_instances N2 ON N1.StudyPatientID = N2.StudyPatientID
	WHERE N1.half_days_since_randomization = N2.half_days_since_randomization - 1
	;


	### v2IVInstancesM ###
	DROP TABLE COVID_PHI.v2IVInstancesM;
	CREATE TABLE COVID_PHI.v2IVInstancesM AS
		SELECT
			EIA.fin, 
			EIA.stay_count, 
			SD.StudyPatientId, SD.study_day,
			SD.RandomizationTime_utc,
			vent_dt_utc,
			SD.last_update, SD.RandomizationType,
			documented_text, source_query
		FROM 			
			(SELECT P.encntr_id, O.event_utc AS vent_dt_utc, O.StudyPatientID, 
				P.result_val AS documented_text, O.documented_source AS source_query
			FROM REMAP.v3OrganSupportInstance O
			JOIN CT_DATA.CE_PHYSIO P ON O.event_id = P.EVENT_ID 
			WHERE support_type = 'IMV'
			) AS CEP
			JOIN COVID_PHI.v2EnrolledIcuAdmitsM EIA ON (CEP.encntr_id = EIA.encntr_id)
			JOIN COVID_PHI.v2StudyDayM SD ON (EIA.StudyPatientId = SD.StudyPatientId AND vent_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		WHERE 
			vent_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)		
	;
	
	
	### v2IVInstancesS ###
	DROP TABLE COVID_PHI.v2IVInstancesS;
	CREATE TABLE COVID_PHI.v2IVInstancesS AS
		SELECT
			EIA.fin, 
			EIA.stay_count, 
			SD.StudyPatientId, SD.study_day,
			SD.RandomizationTime_utc,
			vent_dt_utc,
			SD.last_update, SD.RandomizationType,
			documented_text, source_query
		FROM 			
			(SELECT P.encntr_id, O.event_utc AS vent_dt_utc, O.StudyPatientID, 
				P.result_val AS documented_text, O.documented_source AS source_query
			FROM REMAP.v3OrganSupportInstance O
			JOIN CT_DATA.CE_PHYSIO P ON O.event_id = P.EVENT_ID 
			WHERE support_type = 'IMV'
			) AS CEP
			JOIN COVID_PHI.v2EnrolledIcuAdmitsS EIA ON (CEP.encntr_id = EIA.encntr_id)
			JOIN COVID_PHI.v2StudyDayS SD ON (EIA.StudyPatientId = SD.StudyPatientId AND vent_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		WHERE 
			vent_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)		
	;	

	### v2RRTInstanceM ###
	DROP TABLE COVID_PHI.v2RRTInstancesM;
	CREATE TABLE COVID_PHI.v2RRTInstancesM AS
		SELECT DISTINCT
			EIA.fin, 
			EIA.stay_count, 
			SD.StudyPatientId, SD.study_day,
			SD.RandomizationTime_utc,
			rrt_dt_utc,
			SD.last_update, SD.RandomizationType
		FROM 
			(SELECT P.encntr_id, O.event_utc AS rrt_dt_utc, O.StudyPatientID
			FROM REMAP.v3RRTInstance O
			JOIN CT_DATA.CE_PHYSIO P ON O.event_id = P.EVENT_ID 
			) AS CEIO
			JOIN COVID_PHI.v2EnrolledIcuAdmitsM EIA ON (CEIO.encntr_id = EIA.encntr_id)
			JOIN COVID_PHI.v2StudyDayM SD ON (EIA.StudyPatientId = SD.StudyPatientId AND rrt_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		WHERE 
			rrt_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)	
	;

	### v2RRTInstanceS ###
	DROP TABLE COVID_PHI.v2RRTInstancesS;
	CREATE TABLE COVID_PHI.v2RRTInstancesS AS
		SELECT DISTINCT
			EIA.fin, 
			EIA.stay_count, 
			SD.StudyPatientId, SD.study_day,
			SD.RandomizationTime_utc,
			rrt_dt_utc,
			SD.last_update, SD.RandomizationType
		FROM 
			(SELECT P.encntr_id, O.event_utc AS rrt_dt_utc, O.StudyPatientID
			FROM REMAP.v3RRTInstance O
			JOIN CT_DATA.CE_PHYSIO P ON O.event_id = P.EVENT_ID 
			) AS CEIO
			JOIN COVID_PHI.v2EnrolledIcuAdmitsS EIA ON (CEIO.encntr_id = EIA.encntr_id)
			JOIN COVID_PHI.v2StudyDayS SD ON (EIA.StudyPatientId = SD.StudyPatientId AND rrt_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		WHERE 
			rrt_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)	
	;

	### v2SofaInstancesM ###
	DROP TABLE COVID_PHI.v2SofaInstancesM;
	CREATE TABLE COVID_PHI.v2SofaInstancesM AS
		SELECT StudyPatientID, study_day, MAX(score) AS score
		FROM REMAP.v3CalculatedSOFA
		WHERE RandomizationType = 'Moderate'
		GROUP BY StudyPatientID, study_day
		ORDER BY StudyPatientID, study_day
	;

	### v2SofaInstancesS ###
	DROP TABLE COVID_PHI.v2SofaInstancesS;
	CREATE TABLE COVID_PHI.v2SofaInstancesS AS
		SELECT StudyPatientID, study_day, MAX(score) AS score
		FROM REMAP.v3CalculatedSOFA
		WHERE RandomizationType = 'Severe'
		GROUP BY StudyPatientID, study_day
		ORDER BY StudyPatientID, study_day
	;

	### v2HourlyFiO2MeasurementsM ###
	DROP TABLE COVID_PHI.v2HourlyFiO2MeasurementsM;
	CREATE TABLE COVID_PHI.v2HourlyFiO2MeasurementsM
		SELECT C.StudyPatientID, SD.study_day, NULL AS study_hour, C.event_utc AS event_time_utc,
			C.result_float AS result_val, C.fio2_source
		FROM REMAP.v3CalculatedHourlyFiO2 C
		JOIN COVID_PHI.v2StudyDayM SD 
			ON (C.StudyPatientId = SD.StudyPatientId AND event_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		ORDER BY StudyPatientID, study_day, event_time_utc
	;

	### v2HourlyFiO2MeasurementsS ###
	DROP TABLE COVID_PHI.v2HourlyFiO2MeasurementsS;
	CREATE TABLE COVID_PHI.v2HourlyFiO2MeasurementsS
		SELECT C.StudyPatientID, SD.study_day, NULL AS study_hour, C.event_utc AS event_time_utc,
			C.result_float AS result_val, C.fio2_source
		FROM REMAP.v3CalculatedHourlyFiO2 C
		JOIN COVID_PHI.v2StudyDayS SD ON (C.StudyPatientId = SD.StudyPatientId AND event_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		ORDER BY StudyPatientID, study_day, event_time_utc
	;

	### COVID_PHI.v2testDailyCRFM ###
	DROP TABLE COVID_PHI.v2testDailyCRFM;
	CREATE TABLE COVID_PHI.v2testDailyCRFM
		SELECT * FROM COVID_PHI.v2DailyCRFM
	;
	
	###  COVID_PHI.v2testDailyCRFS ###
	DROP TABLE COVID_PHI.v2testDailyCRFS;
	CREATE TABLE COVID_PHI.v2testDailyCRFS
		SELECT * FROM COVID_PHI.v2DailyCRFS
	;
	
	
/* #################### BELOW HERE IS NOT V3 Optimized ###################### */

### COVID_PHI.v2ApacheeTemperaturesS ###
DROP TABLE COVID_PHI.v2ApacheeTemperaturesS; 
CREATE TABLE COVID_PHI.v2ApacheeTemperaturesS
	SELECT 		
		EP.StudyPatientId, EP.fin, EP.RandomizedSevere_utc,
		measurement.ENCNTR_ID, 
		measurement.RESULT_VAL,	
		event_time_utc,
		EVENT_CD, RESULT_UNITS_CD, 
		measurement.event_end_dt_tm AS event_time_local
	FROM
		(SELECT ENCNTR_ID, RESULT_VAL,EVENT_CD, RESULT_UNITS_CD, event_end_dt_tm,
		 	CONVERT_TZ(event_end_dt_tm,'-04:00','+00:00') AS event_time_utc
			FROM CT_DATA.CE_PHYSIO 
			WHERE encntr_id IN (SELECT DISTINCT encntr_id FROM COVID_PHI.v2EnrolledIcuAdmitsS)
				AND event_cd IN (SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION
					WHERE sub_standard_meaning IN ('Temperature (conversion)', 'Temperature (metric)')) 
		UNION
		SELECT ENCNTR_ID, ROUND(((RESULT_VAL-32) * 5/9), 2) AS RESULT_VAL, EVENT_CD, NULL as RESULT_UNITS_CD, event_end_dt_tm,
		 	CONVERT_TZ(event_end_dt_tm,'-04:00','+00:00') AS event_time_utc
			FROM CT_DATA.CE_PHYSIO 
			WHERE encntr_id IN (SELECT DISTINCT encntr_id FROM COVID_PHI.v2EnrolledIcuAdmitsS)
				AND event_cd IN (SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION
					WHERE sub_standard_meaning IN ('Temperature')) 
		) AS measurement
		LEFT JOIN COVID_PHI.v2EnrolledPerson EP ON (measurement.encntr_id = EP.encntr_id)
	WHERE	
		measurement.event_time_utc BETWEEN date_add(EP.RandomizedSevere_utc, INTERVAL -24 HOUR) AND EP.RandomizedSevere_utc
;

### COVID_PHI.v2ApacheeTemperatureSitesS ###
DROP TABLE COVID_PHI.v2ApacheeTemperatureSitesS; 
CREATE TABLE COVID_PHI.v2ApacheeTemperatureSitesS
	SELECT 		
		measurement.ENCNTR_ID, 
		measurement.RESULT_VAL AS measurement_site,	
		measurement.event_end_dt_tm AS event_time_local,
		If(measurement.RESULT_VAL IN (SELECT source_text FROM COVID_SUPPLEMENT.TEXT_STANDARDIZATION 
			WHERE sub_standard_meaning = 'Core Temperature'), 0, 1) AS preferred_rank
	FROM
		(SELECT *, CONVERT_TZ(event_end_dt_tm,'-04:00','+00:00') AS event_time_utc
			FROM CT_DATA.CE_PHYSIO 
			WHERE encntr_id IN (SELECT DISTINCT encntr_id FROM COVID_PHI.v2EnrolledIcuAdmitsS)
				AND event_cd IN (SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION
					WHERE sub_standard_meaning IN ('Temperature (site)'))
		) AS measurement
		LEFT JOIN COVID_PHI.v2EnrolledPerson EP ON (measurement.encntr_id = EP.encntr_id)
	WHERE
		measurement.event_time_utc BETWEEN date_add(EP.RandomizedSevere_utc, INTERVAL -24 HOUR) AND EP.RandomizedSevere_utc	
;

### COVID_PHI.v2ApacheeBloodPressuresS ###
DROP TABLE COVID_PHI.v2ApacheeBloodPressuresS; 
CREATE TABLE COVID_PHI.v2ApacheeBloodPressuresS
	SELECT 		
		EP.StudyPatientId, EP.fin, EP.RandomizedSevere_utc,
		measurement.ENCNTR_ID, 
		measurement.RESULT_VAL,	
		event_time_utc,
		EVENT_CD, RESULT_UNITS_CD, 
		measurement.event_end_dt_tm AS event_time_local, 
		if(event_cd IN (SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION WHERE sub_standard_meaning IN ('Blood pressure (arterial diastolic)'))
			,'diastolic', 'systolic') AS bp_type
	FROM
		(SELECT *, CONVERT_TZ(event_end_dt_tm,'-04:00','+00:00') AS event_time_utc
			FROM CT_DATA.CE_PHYSIO 
			WHERE encntr_id IN (SELECT DISTINCT encntr_id FROM COVID_PHI.v2EnrolledIcuAdmitsS)
				AND event_cd IN (SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION
					WHERE sub_standard_meaning IN ('Blood pressure (arterial diastolic)', 'Blood pressure (arterial systolic)'))
		) AS measurement
		LEFT JOIN COVID_PHI.v2EnrolledPerson EP ON (measurement.encntr_id = EP.encntr_id)
	WHERE
		measurement.event_time_utc BETWEEN date_add(EP.RandomizedSevere_utc, INTERVAL -24 HOUR) AND EP.RandomizedSevere_utc	
;

### COVID_PHI.v2ApacheeCO2S ###
DROP TABLE COVID_PHI.v2ApacheeCO2S;
CREATE TABLE COVID_PHI.v2ApacheeCO2S
	SELECT encntr_id, REMAP.to_float(result_val) AS PaCO2_value, CONVERT_TZ(event_end_dt_tm,'-04:00','+00:00') AS PaCO2_dt_utc
					FROM CT_DATA.CE_LAB 
					WHERE encntr_id IN (SELECT DISTINCT encntr_id FROM COVID_PHI.v2EnrolledIcuAdmitsS)
						AND event_cd IN (SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION WHERE sub_standard_meaning IN ('PaCO2'))
						AND REMAP.to_float(result_val) IS NOT null
;


### COVID_PHI.v2ApacheeOxygenationMeasurementsS ###
DROP TABLE COVID_PHI.v2ApacheeOxygenationMeasurementsS;
CREATE TABLE COVID_PHI.v2ApacheeOxygenationMeasurementsS
	SELECT 
		studypatientid, PaO2_FiO2_values.encntr_id, apachee_var, PaO2_value, FiO2_value, PaCO2_value, PaO2_dt_utc 
	FROM 
		(SELECT 
				EP.studypatientid, encntr_id, 'Oxygenation' AS apachee_var, 
				HVS.PaO2_value, HVS.FiO2_value, PaO2_dt_utc 
		FROM 
			COVID_PHI.v2HypoxiaVarS HVS
			LEFT JOIN COVID_PHI.v2EnrolledPerson EP ON (HVS.StudyPatientId = EP.StudyPatientId)
		WHERE 
			HVS.PaO2_dt_utc BETWEEN date_add(EP.RandomizedSevere_utc, INTERVAL -24 HOUR) AND EP.RandomizedSevere_utc
		) AS PaO2_FiO2_values
		LEFT JOIN COVID_PHI.v2ApacheeCO2S 
		 AS PaCO2_values ON (PaO2_FiO2_values.encntr_id = PaCO2_values.encntr_id AND PaO2_FiO2_values.PaO2_dt_utc = PaCO2_values.PaCO2_dt_utc)
		
;

### COVID_PHI.v2ApacheeOxygenationDevicesS ###
DROP TABLE COVID_PHI.v2ApacheeOxygenationDevicesS;
CREATE TABLE COVID_PHI.v2ApacheeOxygenationDevicesS
	SELECT 
		devices.studypatientid, delivery_device_text, device_dt_utc, device_apachee_type, O2_Rate
	FROM	
		(SELECT EP.studypatientid, '<O2 rate and FiO2 requirements for HFNC>' AS delivery_device_text, 
				NC_instance.hfnc_dt_utc AS device_dt_utc, 'NC' AS device_apachee_type, EP.RandomizedSevere_utc
			FROM
				(SELECT * FROM COVID_PHI.v2HFNCInstancesM 
					UNION
				SELECT * FROM COVID_PHI.v2HFNCInstancesS
				) AS NC_instance
				LEFT JOIN COVID_PHI.v2EnrolledPerson EP ON (NC_instance.fin = EP.fin)
			WHERE
				NC_instance.hfnc_dt_utc BETWEEN date_add(EP.RandomizedSevere_utc, INTERVAL -24 HOUR) AND EP.RandomizedSevere_utc				
		UNION
			SELECT EP.studypatientid, result_val AS delivery_device_text, device_dt_utc, sub_standard_meaning AS device_apachee_type, EP.RandomizedSevere_utc
			FROM (SELECT 
						encntr_id, result_val, CONVERT_TZ(event_end_dt_tm,'-04:00','+00:00') AS device_dt_utc 
					 FROM 
					 	CT_DATA.CE_PHYSIO 
					 WHERE 
						event_cd IN (SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION WHERE sub_standard_meaning = 'Oxygen therapy delivery device')
							AND
						encntr_id IN (SELECT DISTINCT encntr_id FROM COVID_PHI.v2EnrolledIcuAdmitsS)
							AND
						result_val IN (SELECT source_text FROM COVID_SUPPLEMENT.TEXT_STANDARDIZATION WHERE source_table = 'CE_PHYSIO' AND sub_standard_meaning IN ('NC', 'Mask', 'Nonrebreather', 'Prebreather'))
				) AS device_documented
				LEFT JOIN COVID_PHI.v2EnrolledPerson EP ON (device_documented.encntr_id = EP.encntr_id)
				JOIN (SELECT sub_standard_meaning, source_text
						FROM COVID_SUPPLEMENT.TEXT_STANDARDIZATION WHERE source_table = 'CE_PHYSIO' AND sub_standard_meaning IN ('NC', 'Mask', 'Nonrebreather', 'Prebreather')
				) AS device_type ON (device_documented.result_val = device_type.source_text) 
			WHERE
				device_documented.device_dt_utc BETWEEN date_add(EP.RandomizedSevere_utc, INTERVAL -24 HOUR) AND EP.RandomizedSevere_utc	
		) AS devices
		JOIN (
			SELECT 
				EP.studypatientid, result_val AS O2_rate, O2_rate_dt_utc
			FROM 
				((SELECT encntr_id, REMAP.to_float(result_val) AS result_val, CONVERT_TZ(event_end_dt_tm,'-04:00','+00:00') AS O2_rate_dt_utc FROM CT_DATA.CE_PHYSIO 
					WHERE 
						event_cd IN (SELECT DISTINCT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION WHERE sub_standard_meaning = 'Oxygen Flow Rate')
							AND
						encntr_id IN (SELECT DISTINCT encntr_id FROM COVID_PHI.v2EnrolledIcuAdmitsS)	
				) AS CEP
				LEFT JOIN COVID_PHI.v2EnrolledPerson EP ON (CEP.encntr_id = EP.encntr_id))
			WHERE 
				O2_rate_dt_utc BETWEEN date_add(EP.RandomizedSevere_utc, INTERVAL -24 HOUR) AND EP.RandomizedSevere_utc
				AND 
				result_val IS NOT NULL 
		) AS O2_document ON (devices.studypatientid = O2_document.studypatientid AND device_dt_utc = O2_rate_dt_utc)

;

### COVID_PHI.v2ApacheeOxygenationS ###
DROP TABLE COVID_PHI.v2ApacheeOxygenationS; 
CREATE TABLE COVID_PHI.v2ApacheeOxygenationS
	SELECT 
		*, if (adjusted_FiO2_value < 50, PaO2_value, (7.13*adjusted_FiO2_value)-(PaCO2_value/0.8)-PaO2_value) AS result_val, 
		if (adjusted_FiO2_value < 50, 'PaO2', 'A-aDO2') AS result_type 
		FROM
			(SELECT 
				AOM.studypatientid, AOM.encntr_id, PaO2_value, PaCO2_value, FiO2_value, apachee_var, PaO2_dt_utc AS event_dt_utc, 
				delivery_device_text, device_apachee_type, O2_rate, device_dt_utc,
				CASE
					WHEN device_apachee_type = 'NC' THEN 
						CASE 
							WHEN O2_rate <= 1 THEN 24
							WHEN O2_rate <= 2 THEN 28
							WHEN O2_rate <= 3 THEN 32
							WHEN O2_rate <= 4 THEN 36
							ELSE FiO2_value
						END
					WHEN device_apachee_type = 'Mask' THEN
						CASE 
							WHEN O2_rate BETWEEN 8 AND 15 THEN 70
							ELSE FiO2_value
						END
					WHEN device_apachee_type = 'Nonrebreather' THEN
						CASE 
							WHEN O2_rate BETWEEN 8 AND 15 THEN 95
							ELSE FiO2_value
						END
					WHEN device_apachee_type = 'Prebreather' THEN
						CASE 
							WHEN O2_rate BETWEEN 8 AND 15 THEN 70
							ELSE FiO2_value
						END
					ELSE FiO2_value
				END AS adjusted_FiO2_value
			FROM
				COVID_PHI.v2ApacheeOxygenationMeasurementsS AOM
				LEFT JOIN COVID_PHI.v2ApacheeOxygenationDevicesS AOD ON (AOM.studypatientid = AOD.studypatientid AND PaO2_dt_utc > device_dt_utc)
			) AS oxygenation_data	
;

SELECT 'updatev2tables is finished' AS Progress;
		
