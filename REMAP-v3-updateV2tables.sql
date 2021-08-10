/*
REMAP-v3-update_v2Tables.sql
created by AndrewJKing.com | @AndrewsJourney

NAVIGATION: 
	ALL are COVID_PHI
	v2EnrolledPerson -> FROM REMAP.v3Participant, REMAP.v3IdMap, REMAP.v3RandomizedModerate, REMAP.v3RandomizedSevere, REMAP.v3LocOrder, REMAP.ManualChange_StartOfHospitalization_utc
	v2EnrolledIcuAdmitsM -> FROM REMAP.v3IcuStay, REMAP.v3RandomizedModerate, REMAP.v3LocOrder, REMAP.v3Participant, REMAP.v3IdMap
	v2EnrolledIcuAdmitsS -> FROM REMAP.v3IcuStay, REMAP.v3RandomizedSevere, REMAP.v3LocOrder, REMAP.v3Participant, REMAP.v3IdMap
	v2StateHypoxiaAtEnrollM -> FROM REMAP.v3RandomizedModerate, REMAP.v3CalculatedStateHypoxiaAtEnroll
	v2StateHypoxiaAtEnrollS -> FROM REMAP.v3RandomizedSevere, REMAP.v3CalculatedStateHypoxiaAtEnroll
	REMAP.v3tempHypoxiaVar -> FROM REMAP.v3CalculatedPEEPjoinFiO2, REMAP.v3CalculatedPFratio
	v2HypoxiaVarM -> FROM REMAP.v3tempHypoxiaVar, REMAP.v3RandomizedModerate
	v2HypoxiaVarS -> FROM REMAP.v3tempHypoxiaVar, REMAP.v3RandomizedSevere
	v2StudyDayM -> FROM REMAP.v3StudyDay, REMAP.v3RandomizedModerate
	v2StudyDayS -> FROM REMAP.v3StudyDay, REMAP.v3RandomizedSevere
	v2VasoInstanceM -> FROM REMAP.v3OrganSupportInstance, CT_DATA.MAR_AD, CT_DATA.CODE_VALUE, COVID_PHI.v2EnrolledIcuAdmitsM, COVID_PHI.v2StudyDayM
	v2VasoInstanceS -> FROM REMAP.v3OrganSupportInstance, CT_DATA.MAR_AD, CT_DATA.CODE_VALUE, COVID_PHI.v2EnrolledIcuAdmitsS, COVID_PHI.v2StudyDayS
	v2HFNCInstanceM -> FROM REMAP.v3OrganSupportInstance, CT_DATA.CE_PHYSIO, CT_DATA.CODE_VALUE, COVID_PHI.v2EnrolledIcuAdmitsM, COVID_PHI.v2StudyDayM
	v2HFNCInstanceS -> FROM REMAP.v3OrganSupportInstance, CT_DATA.CE_PHYSIO, CT_DATA.CODE_VALUE, COVID_PHI.v2EnrolledIcuAdmitsS, COVID_PHI.v2StudyDayS
	v2RelaxedHFNCInstanceM -> FROM REMAP.v3SupplementalOxygenInstance, CT_DATA.CE_PHYSIO, CT_DATA.CODE_VALUE, COVID_PHI.v2EnrolledIcuAdmitsM, COVID_PHI.v2StudyDayM
	v2RelaxedHFNCInstanceS -> FROM REMAP.v3SupplementalOxygenInstance, CT_DATA.CE_PHYSIO, CT_DATA.CODE_VALUE, COVID_PHI.v2EnrolledIcuAdmitsS, COVID_PHI.v2StudyDayS
	v2ECMOInstanceM -> FROM REMAP.v3OrganSupportInstance, CT_DATA.CE_PHYSIO, CT_DATA.CODE_VALUE, COVID_PHI.v2EnrolledIcuAdmitsM, COVID_PHI.v2StudyDayM
	v2ECMOInstanceS -> FROM REMAP.v3OrganSupportInstance, CT_DATA.CE_PHYSIO, CT_DATA.CODE_VALUE, COVID_PHI.v2EnrolledIcuAdmitsS, COVID_PHI.v2StudyDayS
	v2NivInstancesM -> FROM REMAP.v3OrganSupportInstance, CT_DATA.CE_PHYSIO, COVID_PHI.v2EnrolledIcuAdmitsM, COVID_PHI.v2StudyDayM
	v2NivInstancesS -> FROM REMAP.v3OrganSupportInstance, CT_DATA.CE_PHYSIO, COVID_PHI.v2EnrolledIcuAdmitsS, COVID_PHI.v2StudyDayS
	v2IVInstancesM -> FROM REMAP.v3OrganSupportInstance, CT_DATA.CE_PHYSIO, COVID_PHI.v2EnrolledIcuAdmitsM, COVID_PHI.v2StudyDayM
	v2IVInstancesS -> FROM REMAP.v3OrganSupportInstance, CT_DATA.CE_PHYSIO, COVID_PHI.v2EnrolledIcuAdmitsS, COVID_PHI.v2StudyDayS
	v2RRTInstanceM -> FROM REMAP.v3RRTInstance, CT_DATA.CE_PHYSIO, CT_DATA.CE_INTAKE_OUTPUT_RESULT, COVID_PHI.v2EnrolledIcuAdmitsM, COVID_PHI.v2StudyDayM
	v2RRTInstanceS -> FROM REMAP.v3RRTInstance, CT_DATA.CE_PHYSIO, CT_DATA.CE_INTAKE_OUTPUT_RESULT, COVID_PHI.v2EnrolledIcuAdmitsS, COVID_PHI.v2StudyDayS
	v2SofaInstancesM -> FROM REMAP.v3CalculatedSOFA
	v2SofaInstancesS -> FROM REMAP.v3CalculatedSOFA
	v2HourlyFiO2MeasurementsM -> FROM REMAP.v3CalculatedHourlyFiO2, COVID_PHI.v2StudyDayM
	v2HourlyFiO2MeasurementsS -> FROM REMAP.v3CalculatedHourlyFiO2, COVID_PHI.v2StudyDayS
	v2testDailyCRFM -> FROM COVID_PHI.v2DailyCRFM
	v2testDailyCRFS -> FROM COVID_PHI.v2DailyCRFS
	v2ApacheeVars -> FROM REMAP.v3Physio, REMAP.v3PhysioStr, REMAP.v3RandomizedSevere, REMAP.v3CalculatedPFratio, REMAP.v3Lab,  CA_DB.INTAKE_FORM, COVID_PHI.v2EnrolledPerson
	# VIEW: COVID_PHI.Outcome_day14 (DEPRICIATED)#
*/

	### Create v2EnrolledPerson ###
	DROP TABLE COVID_PHI.v2EnrolledPerson; 
	CREATE TABLE COVID_PHI.v2EnrolledPerson
		SELECT P.PERSON_ID, P.MRN, I.ENCNTR_ID, I.FIN, P.screendate_utc, P.StudyPatientID, 
			M.randomized_utc AS RandomizedModerate_utc, S.randomized_utc AS RandomizedSevere_utc, 
			H.StartOfHospitalization_utc, 
			H.EndOfHospitalization_utc,
			H.DeceasedAtDischarge,
			P.REGIMEN, CURRENT_TIMESTAMP AS last_update 
		FROM REMAP.v3Participant P
		JOIN REMAP.v3IdMap I ON P.StudyPatientID = I.StudyPatientID
		LEFT JOIN REMAP.v3RandomizedModerate M ON P.StudyPatientID = M.StudyPatientID
		LEFT JOIN REMAP.v3RandomizedSevere S ON P.StudyPatientID = S.StudyPatientID
		LEFT JOIN REMAP.v3Hospitalization H	ON P.StudyPatientID = H.StudyPatientID
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
			MAX(H.FiO2_float) AS FiO2_value, if(MAX(H.FiO2_float) IS NULL, NULL, H.FiO2_utc) AS FiO2_dt_utc,
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
			MAX(H.FiO2_float) AS FiO2_value, if(MAX(H.FiO2_float) IS NULL, NULL, H.FiO2_utc) AS FiO2_dt_utc,
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

	### v2RelaxedHFNCInstanceM ###
	DROP TABLE COVID_PHI.v2RelaxedHFNCInstancesM;
	CREATE TABLE COVID_PHI.v2RelaxedHFNCInstancesM AS 
		SELECT DISTINCT
			EIA.fin, EIA.stay_count, 
			SD.StudyPatientId, SD.study_day,
			SD.RandomizationTime_utc, 
			hfnc_dt_utc, CURRENT_TIMESTAMP as last_update, SD.RandomizationType
		FROM
			(SELECT 	P.encntr_id, SO.event_utc AS hfnc_dt_utc, SO.StudyPatientID
			FROM REMAP.v3SupplementalOxygenInstance SO
			JOIN CT_DATA.CE_PHYSIO P ON SO.event_id = P.EVENT_ID 
			WHERE support_type = 'relaxedHF'
			) AS device
			JOIN COVID_PHI.v2EnrolledIcuAdmitsM EIA ON (device.encntr_id = EIA.encntr_id)
			JOIN COVID_PHI.v2StudyDayM SD ON (EIA.StudyPatientId = SD.StudyPatientId 
				AND hfnc_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		WHERE 
			hfnc_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)
		;


	### v2RelaxedHFNCInstanceS ###
	DROP TABLE COVID_PHI.v2RelaxedHFNCInstancesS;
	CREATE TABLE COVID_PHI.v2RelaxedHFNCInstancesS AS 
	SELECT DISTINCT
			EIA.fin, EIA.stay_count, 
			SD.StudyPatientId, SD.study_day,
			SD.RandomizationTime_utc, 
			hfnc_dt_utc, CURRENT_TIMESTAMP as last_update, SD.RandomizationType
		FROM
			(SELECT 	P.encntr_id, SO.event_utc AS hfnc_dt_utc, SO.StudyPatientID
			FROM REMAP.v3SupplementalOxygenInstance SO
			JOIN CT_DATA.CE_PHYSIO P ON SO.event_id = P.EVENT_ID 
			WHERE support_type = 'relaxedHF'
			) AS device
			JOIN COVID_PHI.v2EnrolledIcuAdmitsS EIA ON (device.encntr_id = EIA.encntr_id)
			JOIN COVID_PHI.v2StudyDayS SD ON (EIA.StudyPatientId = SD.StudyPatientId 
				AND hfnc_dt_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		WHERE 
			hfnc_dt_utc BETWEEN date_add(EIA.start_dt_utc, INTERVAL -24 HOUR) AND date_add(EIA.end_dt_utc, INTERVAL 24 HOUR)
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
	SELECT *, '<DEPRICIATED>' AS linked_half_day
	FROM NIV_instances
	/*
	SELECT N1.*, N2.half_days_since_randomization AS linked_half_day
	FROM NIV_instances N1
	LEFT JOIN NIV_instances N2 ON N1.StudyPatientID = N2.StudyPatientID
	WHERE N2.StudyPatientID IS NULL OR N1.half_days_since_randomization = N2.half_days_since_randomization + 1
	UNION 
	SELECT N1.*, N2.half_days_since_randomization AS linked_half_day
	FROM NIV_instances N1
	LEFT JOIN NIV_instances N2 ON N1.StudyPatientID = N2.StudyPatientID
	WHERE N2.StudyPatientID IS NULL OR N1.half_days_since_randomization = N2.half_days_since_randomization - 1
	*/
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
	SELECT *, '<DEPRICIATED>' AS linked_half_day
	FROM NIV_instances
	/*
	SELECT N1.*, N2.half_days_since_randomization AS linked_half_day
	FROM NIV_instances N1
	LEFT JOIN NIV_instances N2 ON N1.StudyPatientID = N2.StudyPatientID
	WHERE N2.StudyPatientID IS NULL OR N1.half_days_since_randomization = N2.half_days_since_randomization + 1
	UNION 
	SELECT N1.*, N2.half_days_since_randomization AS linked_half_day
	FROM NIV_instances N1
	LEFT JOIN NIV_instances N2 ON N1.StudyPatientID = N2.StudyPatientID
	WHERE N2.StudyPatientID IS NULL OR N1.half_days_since_randomization = N2.half_days_since_randomization - 1
	*/
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
			WHERE documented_source = 'Physio' 
			UNION  
			SELECT I.encntr_id, O.event_utc AS rrt_dt_utc, O.StudyPatientID
			FROM REMAP.v3RRTInstance O
			JOIN CT_DATA.CE_INTAKE_OUTPUT_RESULT I ON O.event_id = I.EVENT_ID
			WHERE documented_source = 'IO'  
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
			WHERE documented_source = 'Physio' 
			UNION  
			SELECT I.encntr_id, O.event_utc AS rrt_dt_utc, O.StudyPatientID
			FROM REMAP.v3RRTInstance O
			JOIN CT_DATA.CE_INTAKE_OUTPUT_RESULT I ON O.event_id = I.EVENT_ID
			WHERE documented_source = 'IO' 
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
	
	

DROP TABLE COVID_PHI.v2ApacheeVarS;
CREATE TABLE COVID_PHI.v2ApacheeVarS
	## temperature ##
	SELECT temp.StudyPatientId, 'Temperature' AS apachee_var, RESULT_VAL,
		ROUND(result_val+0.1,0) AS rounded_result_val, temp.event_utc AS event_time_utc,
		CASE
		    WHEN result_val >= 41 THEN 4
		    WHEN result_val >= 39 THEN 3
		    WHEN result_val >= 38.5 THEN 1
		    WHEN result_val >= 36 THEN 0
		    WHEN result_val >= 34 THEN 1
		    WHEN result_val >= 32 THEN 2
		    WHEN result_val >= 30 THEN 3
		    WHEN result_val < 30 THEN 4
		    ELSE NULL
		END AS points,
		preferred_rank	
	FROM
		(SELECT 
			StudyPatientID, event_utc, 
			if(result_float > 48, ROUND(((result_float-32) * 5/9), 2), result_float) AS result_val 
		FROM REMAP.v3Physio 
		WHERE sub_standard_meaning IN ('Temperature (conversion)', 'Temperature (metric)', 'Temperature')
		) AS temp
	JOIN
		(SELECT StudyPatientID, event_utc, if(result_str = 'Core Temperature', 0, 1) AS preferred_rank
		FROM REMAP.v3PhysioStr 
		WHERE sub_standard_meaning IN ('Temperature (site)')
		) AS site ON temp.studypatientid = site.studypatientid AND temp.event_utc = site.event_utc
	JOIN REMAP.v3RandomizedSevere R ON temp.StudyPatientID = R.StudyPatientID 
		AND temp.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc	
	UNION
	## oxygenation ##
	SELECT 
		StudyPatientID, 'Oxygenation' AS apachee_var, RESULT_VAL, rounded_result_val, 
		event_utc as event_time_utc, 
		CASE
		    WHEN result_type = 'A-aDO2' and rounded_result_val >= 500 THEN 4
		    WHEN result_type = 'A-aDO2' and rounded_result_val >= 350 THEN 3
		    WHEN result_type = 'A-aDO2' and rounded_result_val >= 200 THEN 2
		    WHEN result_type = 'A-aDO2' and rounded_result_val < 200 THEN 0
		    WHEN result_type = 'PaO2' and rounded_result_val > 70 THEN 0
		    WHEN result_type = 'PaO2' and rounded_result_val >= 61 THEN 1
		    WHEN result_type = 'PaO2' and rounded_result_val >= 55 THEN 3
		    WHEN result_type = 'PaO2' and rounded_result_val < 55 THEN 4
		    ELSE NULL
		END AS points,
		0 AS preferred_rank
	FROM
		(SELECT *, ROUND(result_val+0.1, 0) AS rounded_result_val
		FROM	
			(SELECT 
				joined.StudyPatientID, event_utc,
				if (Fio2_float IS NULL OR Fio2_float < 50, PaO2_float, (7.13*Fio2_float)-(PaCO2_float/0.8)-PaO2_float) AS result_val, 
				if (Fio2_float IS NULL OR Fio2_float < 50, 'PaO2', 'A-aDO2') AS result_type 
			FROM
				(SELECT P.StudyPatientID, C.event_utc, P.PaO2_float, C.result_float AS PaCO2_float, P.Fio2_float 
				FROM  
					(SELECT P.* FROM REMAP.v3CalculatedPFratio P JOIN REMAP.v3RandomizedSevere R ON P.StudyPatientID = R.StudyPatientID
					WHERE P.PaO2_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc
					) AS P
					JOIN 	
					(SELECT L.* 
					FROM REMAP.v3Lab L JOIN REMAP.v3RandomizedSevere R ON L.StudyPatientID = R.StudyPatientID
					WHERE sub_standard_meaning IN ('PaCO2') 
						AND L.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc
					) AS C ON P.StudyPatientID = C.StudyPatientID AND P.PaO2_utc = C.event_utc
				) AS joined
			) AS calculated
		) AS rounded					
	UNION
	## MAP ##
	SELECT 
		StudyPatientId, 'MAP' AS apachee_var, result_float as RESULT_VAL, 
		ROUND(result_float+0.1, 0) as rounded_result_val, event_utc AS event_time_utc,
		CASE
	    WHEN rounded_result_val >= 160 THEN 4
	    WHEN rounded_result_val >= 130 THEN 3
	    WHEN rounded_result_val >= 110 THEN 2
	    WHEN rounded_result_val >= 70 THEN 0
	    WHEN rounded_result_val >= 50 THEN 2
	    WHEN rounded_result_val < 50 THEN 4
	    ELSE NULL
		END AS points,
		preferred_rank	
	FROM
		(SELECT *, ROUND(result_float+0.1, 0) as rounded_result_val
		FROM  # MAP calculated #
			(SELECT D.STUDYPATIENTID, (D.result_float * 2 + S.result_float)/3 AS result_float, D.event_utc, 1 AS preferred_rank
			FROM REMAP.v3Physio D
			JOIN REMAP.v3Physio S ON (D.StudyPatientID = S.StudyPatientID AND D.event_utc = S.event_utc)	
			JOIN REMAP.v3RandomizedSevere R ON D.StudyPatientID = R.StudyPatientID
			WHERE D.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc		
				AND D.sub_standard_meaning IN ('Blood pressure (arterial diastolic)') 
				AND S.sub_standard_meaning IN ('Blood pressure (arterial systolic)')
			UNION # MAP as documented #
			SELECT P.StudyPatientID, P.result_float, P.event_utc, 0 AS preferred_rank	
			FROM REMAP.v3Physio P
			JOIN REMAP.v3RandomizedSevere R ON P.StudyPatientID = R.StudyPatientID
			WHERE sub_standard_meaning IN ('Blood Pressure (MAP)', 'Blood Pressure (mean)')
				AND P.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc
			) AS joined
		) AS rounded
		UNION
		## heart rate ##
		SELECT 
			M.StudyPatientId, 'Heart Rate' AS apachee_var, RESULT_VAL, 
			rounded_result_val, event_time_utc,
			CASE
			    WHEN rounded_result_val >= 180 THEN 4
			    WHEN rounded_result_val >= 140 THEN 3
			    WHEN rounded_result_val >= 110 THEN 2
			    WHEN rounded_result_val >= 70 THEN 0
			    WHEN rounded_result_val >= 55 THEN 2
			    WHEN rounded_result_val >= 40 THEN 3
			    WHEN rounded_result_val < 40 THEN 4
			    ELSE NULL
			END AS points,
			0 AS preferred_rank
		FROM
			(SELECT StudyPatientID, event_utc AS event_time_utc, result_float as RESULT_VAL, 
				ROUND(result_float+.1, 0) as rounded_result_val  
			FROM REMAP.v3Physio 
			WHERE sub_standard_meaning IN ('Heart rate')
			) AS M
		JOIN REMAP.v3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
		WHERE M.event_time_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc
		UNION
		## respiratory rate ##
		SELECT 
			M.StudyPatientId, 'Respiratory Rate' AS apachee_var, RESULT_VAL, 
			rounded_result_val, event_time_utc,
			CASE
			    WHEN rounded_result_val >= 50 THEN 4
			    WHEN rounded_result_val >= 35 THEN 3
			    WHEN rounded_result_val >= 25 THEN 1
			    WHEN rounded_result_val >= 12 THEN 0
			    WHEN rounded_result_val >= 10 THEN 1
			    WHEN rounded_result_val >= 6 THEN 2
			    WHEN rounded_result_val < 6 THEN 4
				 ELSE NULL
			END AS points,
			0 AS preferred_rank
		FROM
			(SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) as rounded_result_val  
			FROM REMAP.v3Physio 
			WHERE sub_standard_meaning IN ('Respiratory rate')
			) AS M
		JOIN REMAP.v3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
		WHERE M.event_time_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc
		UNION
		## arterial pH ##
		SELECT 
			M.StudyPatientId, 'Arterial pH' AS apachee_var, RESULT_VAL, 
			rounded_result_val, event_time_utc,
			CASE
			    WHEN RESULT_VAL >= 7.7 THEN 4
			    WHEN RESULT_VAL >= 7.6 THEN 3
			    WHEN RESULT_VAL >= 7.5 THEN 1
			    WHEN RESULT_VAL >= 7.33 THEN 0
			    WHEN RESULT_VAL >= 7.25 THEN 2
			    WHEN RESULT_VAL >= 7.15 THEN 3
			    WHEN RESULT_VAL < 7.15 THEN 4
			    ELSE NULL
			END AS points,
			0 AS preferred_rank
		FROM
			(SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) as rounded_result_val  
			FROM REMAP.v3Lab 
			WHERE sub_standard_meaning IN ('pH (arterial)', 'pH (arterial temp corrected)', 'pH (arterial iStat)')
			) AS M
		JOIN REMAP.v3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
		WHERE M.event_time_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc		
		UNION
		## arterial pH via serum bicarbonate ##
		SELECT 
			M.StudyPatientId, 'Arterial pH' AS apachee_var, RESULT_VAL, 
			rounded_result_val, event_time_utc,
			CASE
			    WHEN result_val >= 52 THEN 4
			    WHEN result_val >= 41 THEN 3
			    WHEN result_val >= 32 THEN 1
			    WHEN result_val >= 22 THEN 0
			    WHEN result_val >= 18 THEN 2
			    WHEN result_val >= 15 THEN 3
			    WHEN result_val < 15 THEN 4
			    ELSE NULL
			END AS points,
			1 AS preferred_rank
		FROM
			(SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) as rounded_result_val  
			FROM REMAP.v3Lab 
			WHERE sub_standard_meaning IN ('Bicarbonate', 'Bicarbonate (iStat)', 'Bicarbonate (venous calc)', 
				'Bicarbonate (venous iStat calc)', 'Bicarbonate (venous iStat)', 'Bicarbonate (venous)')
			) AS M
		JOIN REMAP.v3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
		WHERE M.event_time_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc	
		UNION
		## serum sodium ##
		SELECT 
			M.StudyPatientId, 'Serum Sodium' AS apachee_var, RESULT_VAL, 
			rounded_result_val, event_time_utc,
			CASE
			    WHEN rounded_result_val >= 180 THEN 4
			    WHEN rounded_result_val >= 160 THEN 3
			    WHEN rounded_result_val >= 155 THEN 2
			    WHEN rounded_result_val >= 150 THEN 1
			    WHEN rounded_result_val >= 130 THEN 0
			    WHEN rounded_result_val >= 120 THEN 2
			    WHEN rounded_result_val >= 111 THEN 3
			    WHEN rounded_result_val < 111 THEN 4
			    ELSE NULL
			END AS points,
			preferred_rank
		FROM
			(SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) AS rounded_result_val, 0 AS preferred_rank
			FROM REMAP.v3Lab 
			WHERE sub_standard_meaning IN ('Sodium')
			UNION
			SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) as rounded_result_val, 1 AS preferred_rank
			FROM REMAP.v3Lab 
			WHERE sub_standard_meaning IN ('Sodium (whole blood) ','Sodium (blood gas)', 'Sodium (arterial)',
				'Sodium (iStat)', 'Sodium (venous iStat)', 'Sodium (arterial iStat)','Sodium (other)')
			) AS M
		JOIN REMAP.v3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
		WHERE M.event_time_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc	
		UNION
		## serum potassium 
		SELECT 
			M.StudyPatientId, 'Serum Potassium' AS apachee_var, RESULT_VAL, 
			rounded_result_val, event_time_utc,
			CASE
			    WHEN result_val >= 7 THEN 4
			    WHEN result_val >= 6 THEN 3
			    WHEN result_val >= 5.5 THEN 1
			    WHEN result_val >= 3.5 THEN 0
			    WHEN result_val >= 3 THEN 1
			    WHEN result_val >= 2.5 THEN 2
			    WHEN result_val < 2.5 THEN 4
			    ELSE NULL
			END AS points,
			preferred_rank
		FROM
			(SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) AS rounded_result_val, 0 AS preferred_rank
			FROM REMAP.v3Lab 
			WHERE sub_standard_meaning IN ('Potassium', 'Potassium (serum)', 'Potassium (plasma)')
			UNION
			SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) as rounded_result_val, 1 AS preferred_rank
			FROM REMAP.v3Lab 
			WHERE sub_standard_meaning IN ('Potassium (whole blood)', 'Potassium (iStat)', 'Potassium (blood gas)',
				'Potassium (arterial)', 'Potassium (venous iStat)', 'Potassium (arterial iStat)','Potassium (venous iStat)')
			) AS M
		JOIN REMAP.v3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
		WHERE M.event_time_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc
		UNION
		## serum creatinine ##
		SELECT 
			M.StudyPatientId, 'Serum Creatinine' AS apachee_var, RESULT_VAL, 
			rounded_result_val, event_time_utc,
			CASE
			    WHEN result_val >= 3.5 THEN if(CRF.has_CRF='yes', 4, 8)
			    WHEN result_val >= 2 THEN if(CRF.has_CRF='yes', 3, 6)
			    WHEN result_val >= 1.5 THEN if(CRF.has_CRF='yes', 2, 4)
			    WHEN result_val >= 0.6 THEN 0
			    WHEN result_val < 0.6 THEN if(CRF.has_CRF='yes', 2, 4)
			    ELSE NULL
			END AS points,
			preferred_rank
		FROM
			(SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) AS rounded_result_val, 0 AS preferred_rank
			FROM REMAP.v3Lab 
			WHERE sub_standard_meaning IN ('Creatinine')
			UNION
			SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) as rounded_result_val, 1 AS preferred_rank
			FROM REMAP.v3Lab 
			WHERE sub_standard_meaning IN ('Creatinine (iStat)', 'Creatinine (other)', 'Creatinine (whole blood)')
			) AS M
		LEFT JOIN 
			(SELECT DISTINCT studypatientid, 'yes' AS has_CRF 
			FROM CA_DB.INTAKE_FORM I 
			JOIN COVID_PHI.v2EnrolledPerson EP ON (I.fin = EP.fin) 
			WHERE comorbidlist LIKE '%chronic renal disease%'
			) AS CRF ON (M.studypatientid = CRF.studypatientid)
		JOIN REMAP.v3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
		WHERE M.event_time_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc
		UNION
		## hematocrit ##
		SELECT 
			M.StudyPatientId, 'Hematocrit' AS apachee_var, RESULT_VAL, 
			rounded_result_val, event_time_utc,
			CASE
			    WHEN result_val >= 60 THEN 4
			    WHEN result_val >= 50 THEN 2
			    WHEN result_val >= 46 THEN 1
			    WHEN result_val >= 30 THEN 0
			    WHEN result_val >= 20 THEN 2
			    WHEN result_val < 20 THEN 4
			    ELSE NULL
			END AS points,
			preferred_rank
		FROM
			(SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) AS rounded_result_val, 0 AS preferred_rank
			FROM REMAP.v3Lab 
			WHERE sub_standard_meaning IN ('Hematocrit','Hematocrit (PFA)')
			UNION
			SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) as rounded_result_val, 1 AS preferred_rank
			FROM REMAP.v3Lab 
			WHERE sub_standard_meaning IN ('Hematocrit (iStat)','Hematocrit (blood gas)', 
				'Hematocrit (arterial iStat)', 'Hematocrit (venous iStat)')
			) AS M
		JOIN REMAP.v3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
		WHERE M.event_time_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc
		UNION
		## white blood cells ##
		SELECT 
			M.StudyPatientId, 'WBC' AS apachee_var, RESULT_VAL, 
			rounded_result_val, event_time_utc,
			CASE
			    WHEN result_val >= 40 THEN 4
			    WHEN result_val >= 20 THEN 2
			    WHEN result_val >= 15 THEN 1
			    WHEN result_val >= 3 THEN 0
			    WHEN result_val >= 1 THEN 2
			    WHEN result_val < 1 THEN 4
			    ELSE NULL
			END AS points,
			0 AS preferred_rank
		FROM
			(SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) as rounded_result_val  
			FROM REMAP.v3Lab 
			WHERE sub_standard_meaning IN ('White blood count')
			) AS M
		JOIN REMAP.v3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
		WHERE M.event_time_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc	
; #SELECT * FROM COVID_PHI.v2ApacheeVarS;
	

SELECT 'updatev2tables is finished' AS Progress;


/*
CREATE OR REPLACE VIEW COVID_PHI.Outcome_day14 AS
	SELECT * FROM REMAP.v3Day14Outcomes ;
*/		
