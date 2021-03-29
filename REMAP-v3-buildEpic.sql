

/*
Dependencies
>REMAP.v3ViewCernerEnrolledPerson2 from REMAP-v3-definedViews.sql
>to_utc(), to_float(), get_prefix(), get_postfix(), get_physio_result_str(),
 to_datetime_epic(), to_unique_orderd_num(), extract_diastolic(), get_physio_result_str_epic(), convert_epic_unit() 
 from REMAP.v3-definedFunctions.sql

*/


/* 
TODO

Codes
	IO	'Transfusions'
	IO	'RRT'
	Physio 'RRT'
	Med 'Vasopressors' 
	Physio	'ECMO'
	Physio 'Oxygen Flow Rate'

Mappings
	Med Vaso meds to meds mentioned in SOFA. 

Tables to implement
	REMAPe.ve3IO
	REMAPe.ve3Med
	REMAPe.ve3OrganSupportInstance # vaso, ecmo, and RRT
	REMAPe.ve3CalculatedSOFA
*/

### pull studypateintids from view ###
DROP TABLE REMAPe.ve3Participant;
CREATE TABLE REMAPe.ve3Participant
	SELECT DISTINCT studypatientid, screendate_utc, regimen, MRN, PERSON_ID, 'Epic' AS source_system
	FROM REMAPe.ve3ViewEpicEnrolledPerson2
ORDER BY studypatientid;
	

# link in related inpatient encntr_ids and fins. 
DROP TABLE REMAPe.ve3IdMap;	
CREATE TABLE REMAPe.ve3IdMap
	SELECT DISTINCT encntr_id, fin, studypatientid, '<epic inpatient>' AS encounter_type
	FROM REMAPe.ve3ViewEpicEnrolledPerson2	
;

### get locations from inpatient encounters ###
DROP TABLE REMAPe.ve3LocOrder;
CREATE TABLE REMAPe.ve3LocOrder
	SELECT DISTINCT IR.*, 0 AS screening_location, IR.end_utc as max_end_of_prior_loc_orders
	FROM (SELECT distinct IM.studypatientid, IM.encntr_id, 
			REMAP.to_utc(REMAP.to_datetime_epic(effective_time)) as beg_utc, 
			CAST('2100-12-31 00:00:00' AS DATETIME) AS end_utc,
			'' as LOC_FACILITY_CD,
			department_id as LOC_NURSE_UNIT_CD,
			if(seq_num_in_enc = '', NULL, CAST(seq_num_in_enc AS UNSIGNED)) AS loc_order
		FROM COVID_PINNACLE.CLARITY_ADT CA 
		JOIN REMAPe.ve3IdMap IM ON CA.pat_enc_csn_id = IM.FIN
		) AS IR
	JOIN COVID_PINNACLE.CLARITY_DEP CD ON IR.LOC_NURSE_UNIT_CD = CD.department_id
	ORDER BY studypatientid, beg_utc
	; 

	## drop where loc_order missing and slot already taken ##
	WITH loc_times AS (
		SELECT studypatientid, beg_utc 
		FROM REMAPe.ve3LocOrder 
		WHERE loc_order IS NOT NULL
	)
	DELETE L 
	FROM REMAPe.ve3LocOrder L
	JOIN loc_times lt ON L.studypatientid = lt.studypatientid AND L.beg_utc = lt.beg_utc 
	WHERE L.loc_order IS NULL
	;

	## fill in null loc_orders ##
	WITH joined_max AS (
		SELECT L1.*, MAX(L2.beg_utc) AS max_beg_before, MAX(L2.loc_order) AS max_loc_before
		FROM (select studypatientid, beg_utc, loc_order FROM REMAPe.ve3LocOrder WHERE loc_order IS NULL) AS L1 
		JOIN REMAPe.ve3LocOrder L2 
		ON L1.studypatientid = L2.studypatientid WHERE L1.beg_utc > L2.beg_utc
		GROUP BY L1.studypatientid, L1.beg_utc, L1.loc_order	
	) 
	UPDATE REMAPe.ve3LocOrder L1
	INNER JOIN joined_max L2 ON L1.studypatientid = L2.studypatientid AND L1.beg_utc > L2.max_beg_before
	SET L1.loc_order = L2.max_loc_before + 1
	WHERE L1.loc_order IS NULL 
	;
	
	## fill in end_utc ##
	UPDATE REMAPe.ve3LocOrder L1
	JOIN REMAPe.ve3LocOrder L2 ON L1.studypatientid = L2.studypatientid AND L1.loc_order = L2.loc_order-1
	SET L1.END_UTC = L2.beg_utc
	WHERE 1 = 1
	;
	
	## fill in end_utc	for last loc_orders ##
	WITH last_data AS (
		SELECT STUDYPATIENTID, MAX(max_event_utc) AS max_event_utc
		FROM
			(SELECT STUDYPATIENTID, MAX(REMAP.to_utc(TAKEN_TIME)) AS max_event_utc
			FROM REMAPe.ve3IdMap I JOIN COVID_PINNACLE.MAR_ADMIN_INFO M ON I.FIN = M.MAR_ENC_CSN
			GROUP BY STUDYPATIENTID
			UNION
			SELECT STUDYPATIENTID, MAX(REMAP.to_utc(RESULT_TIME)) AS max_event_utc
			FROM REMAPe.ve3IdMap I JOIN COVID_PINNACLE.ORDER_RESULTS O ON I.FIN = O.PAT_ENC_CSN_ID
			GROUP BY STUDYPATIENTID
			UNION
			SELECT STUDYPATIENTID, MAX(REMAP.to_utc(REMAP.to_datetime_epic(RECORDED_TIME))) AS max_event_utc
			FROM REMAPe.ve3IdMap I 
			JOIN COVID_PINNACLE.IP_FLWSHT_REC R ON I.encntr_id = R.INPATIENT_DATA_ID
			JOIN COVID_PINNACLE.IP_FLWSHT_MEAS M ON R.FSD_ID = M.FSD_ID
			GROUP BY STUDYPATIENTID
			) AS all_maxes
		GROUP BY STUDYPATIENTID
	)
	UPDATE REMAPe.ve3LocOrder L1
	JOIN last_data L2 ON L1.studypatientid = L2.studypatientid
	SET L1.END_UTC = L2.max_event_utc
	WHERE L1.END_UTC = '2100-12-31 00:00:00'
	;
		
	## indicate which location entriy the pt was at time of screening ##
	UPDATE REMAPe.ve3LocOrder O
		JOIN REMAPe.ve3Participant P ON O.STUDYPATIENTID = P.STUDYPATIENTID
		SET O.screening_location = 1
		WHERE P.screendate_utc BETWEEN O.beg_utc AND O.end_utc
	;

	## update max_end_of_prior_loc_orders to correct values ##
	WITH max_end AS (
		SELECT O1.Studypatientid, O1.loc_order, MAX(O2.end_utc) AS max_end_of_prior_loc_orders
		FROM REMAPe.ve3LocOrder O1 
		LEFT JOIN REMAPe.ve3LocOrder O2 ON O1.studypatientid = O2.studypatientid
		WHERE O2.loc_order <= O1.loc_order
		GROUP BY O1.Studypatientid, O1.loc_order
	)
	UPDATE REMAPe.ve3LocOrder O1 
		LEFT JOIN max_end O2 ON O1.studypatientid = O2.studypatientid AND O1.loc_order = O2.loc_order
		SET O1.max_end_of_prior_loc_orders = O2.max_end_of_prior_loc_orders
	; 

	## identify locations from enrolled hospitalization and remove others ##
	WITH hospitalStayTransition AS  # find hospitalizations that are greater than 12 hours apart
		(SELECT *
		FROM
			(SELECT O1.STUDYPATIENTID, O1.loc_order, TIMESTAMPDIFF(MINUTE, O1.end_utc, O2.beg_utc) AS time_delta_minutes, O1.beg_utc AS beg_for_loc_order, O2.beg_utc AS beg_for_next_loc_order
			FROM REMAPe.ve3LocOrder O1 
			JOIN REMAPe.ve3LocOrder O2 ON O1.STUDYPATIENTID = O2.STUDYPATIENTID AND O1.loc_order = O2.loc_order-1
			) AS IR
		WHERE time_delta_minutes > 60*12  # > 12 hours means end of last hospitalization 
		), screenLoc AS   # find the loc entries where patient was screened
		(SELECT STUDYPATIENTID, loc_order AS screening_loc
			FROM REMAPe.ve3LocOrder		
			WHERE screening_location = 1
		), startLoc AS  # find each patients first location
		(SELECT STUDYPATIENTID, loc_order
			FROM REMAPe.ve3LocOrder		
			WHERE loc_order = 1
		), locAtHospEnd AS  # find the end loc of the enrolled hospitalization
			# filter for transition points that are greater than or equal to the screening point 
			# find the minimum transition point that meets the above criteria
			# will delete all locs greater than this point 
		(SELECT H.STUDYPATIENTID, MIN(H.loc_order) AS h_end_loc 
			FROM hospitalStayTransition H
			JOIN screenLoc S ON H.STUDYPATIENTID = S.STUDYPATIENTID
			WHERE H.loc_order >= S.screening_loc
			GROUP BY H.STUDYPATIENTID
		), subsequentLocAtHospBeg AS  # get the greatest transition loc the is less than the screening point 
			# filter for transition points that are less than the screening point 
			# find the maximum transition point that meets the above criteria
			# add 1 to find the loc that starts the next hospital admission
			# delete all locs below this
		(SELECT H.STUDYPATIENTID, MAX(H.loc_order)+1 AS h_beg_loc 
			FROM hospitalStayTransition H
			JOIN screenLoc S ON H.STUDYPATIENTID = S.STUDYPATIENTID
			WHERE H.loc_order < S.screening_loc
			GROUP BY H.STUDYPATIENTID
		), locAtHospBeg AS  
		(SELECT studypatientid, MAX(h_beg_loc) AS h_beg_loc 
			FROM
				(SELECT * FROM subsequentLocAtHospBeg
				UNION
				SELECT studypatientid, loc_order AS h_beg_loc FROM startLoc
				) AS find_starts
			GROUP BY studypatientid
		), locsForEnrolledHosp AS  # identify the begining loc and ending loc for the enrolled hospitalization
		(SELECT B.STUDYPATIENTID, B.h_beg_loc, E.h_end_loc
			FROM locAtHospBeg B
			LEFT JOIN locAtHospEnd E ON B.studypatientid = E.studypatientid
		) 
	DELETE O
		FROM REMAPe.ve3LocOrder O
		JOIN locsForEnrolledHosp L ON O.STUDYPATIENTID = L.STUDYPATIENTID
		WHERE (O.loc_order < L.h_beg_loc) OR (O.loc_order > h_end_loc)
;

### Remove encounters that are not from an enrolled hospitalization ###
DELETE FROM REMAPe.ve3IdMap
	WHERE ENCNTR_ID NOT IN (SELECT DISTINCT ENCNTR_ID FROM REMAPe.ve3LocOrder)
;

SELECT * FROM REMAPe.ve3Participant;
SELECT * FROM REMAPe.ve3IdMap;
SELECT * FROM REMAPe.ve3LocOrder;


### pull relevant labs ###
DROP TABLE REMAPe.ve3Lab;
CREATE TABLE REMAPe.ve3Lab
	SELECT L.EVENT_ID, M.STUDYPATIENTID, L.event_utc, S.sub_standard_meaning, L.prefix, L.result_float, L.units,
		documented_val, NORMAL_LOW, NORMAL_HIGH
	FROM  
		(SELECT DISTINCT order_proc_id AS event_id, PAT_ENC_CSN_ID AS FIN, COMPONENT_ID AS EVENT_CD,
			REMAP.to_utc(RESULT_TIME) AS event_utc,
			REMAP.get_prefix(ORD_VALUE) as prefix, REMAP.to_float(ORD_VALUE) AS result_float, REFERENCE_UNIT AS units,
			ORD_VALUE AS documented_val, Reference_Low AS NORMAL_LOW, Reference_High AS NORMAL_HIGH
		FROM COVID_PINNACLE.ORDER_RESULTS
		WHERE PAT_ENC_CSN_ID IN (SELECT FIN FROM REMAPe.ve3IdMap)
			AND COMPONENT_ID IN (SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION WHERE source_table IN ('ORDER_RESULTS'))
		) AS L 
		JOIN REMAPe.ve3IdMap M ON L.FIN = M.FIN
		JOIN COVID_SUPPLEMENT.CV_STANDARDIZATION S ON L.EVENT_CD = S.source_cv
	WHERE result_float IS NOT NULL
; 


### pull relevant physio ###
CREATE TABLE REMAPe.tempv3Physio
	SELECT DISTINCT M.FSD_ID AS EVENT_ID, R.INPATIENT_DATA_ID AS encntr_id, M.FLO_MEAS_ID AS EVENT_CD, 
			REMAP.to_datetime_epic(RECORDED_TIME) as EVENT_END_DT_TM, MEAS_VALUE as RESULT_VAL, '' as result_units_cd,
			NULL AS NORMAL_LOW, NULL AS NORMAL_HIGH
		FROM COVID_PINNACLE.IP_FLWSHT_MEAS M
		JOIN COVID_PINNACLE.IP_FLWSHT_REC R ON R.FSD_ID = M.FSD_ID
		WHERE R.INPATIENT_DATA_ID IN (SELECT encntr_id FROM REMAPe.ve3IdMap)
			AND M.FLO_MEAS_ID IN (SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION WHERE source_table IN ('IP_FLWSHT_MEAS'))
	;
	## parse out diastolic bp. (systolic is handard by normal parse function) ##
	INSERT INTO REMAPe.tempv3Physio
		SELECT EVENT_ID, ENCNTR_ID, -5 AS EVENT_CD, EVENT_END_DT_TM, REMAP.extract_diastolic(RESULT_VAL), result_units_cd,
			NULL AS NORMAL_LOW, NULL AS NORMAL_HIGH 
		FROM REMAPe.tempv3Physio 
		WHERE EVENT_CD = 
			(SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION WHERE source_table IN ('IP_FLWSHT_MEAS') 
				AND sub_standard_meaning = 'Blood pressure (systolic)');
	INSERT INTO REMAPe.tempv3Physio
		SELECT EVENT_ID, ENCNTR_ID, -301260 AS EVENT_CD, EVENT_END_DT_TM, REMAP.extract_diastolic(RESULT_VAL), result_units_cd,
			NULL AS NORMAL_LOW, NULL AS NORMAL_HIGH 
		FROM REMAPe.tempv3Physio 
		WHERE EVENT_CD = 301260;
	INSERT INTO REMAPe.tempv3Physio
		SELECT EVENT_ID, ENCNTR_ID, -301280 AS EVENT_CD, EVENT_END_DT_TM, REMAP.extract_diastolic(RESULT_VAL), result_units_cd,
			NULL AS NORMAL_LOW, NULL AS NORMAL_HIGH 
		FROM REMAPe.tempv3Physio 
		WHERE EVENT_CD = 301280;
	## the table for numeric physio value ##
	DROP TABLE REMAPe.ve3Physio;
	CREATE TABLE REMAPe.ve3Physio
		SELECT L.EVENT_ID, M.STUDYPATIENTID, L.event_utc, S.sub_standard_meaning, L.prefix, L.result_float, '' AS units,
			NORMAL_LOW, NORMAL_HIGH
		FROM  
			(SELECT EVENT_ID, ENCNTR_ID, EVENT_CD, REMAP.to_utc(EVENT_END_DT_TM) AS event_utc, 
				REMAP.get_prefix(RESULT_VAL) as prefix, REMAP.to_float(RESULT_VAL) AS result_float, 
				RESULT_VAL as documented_val, NORMAL_LOW, NORMAL_HIGH
			FROM REMAPe.tempv3Physio
			) AS L 
			JOIN REMAPe.ve3IdMap M ON L.ENCNTR_ID = M.ENCNTR_ID
			JOIN COVID_SUPPLEMENT.CV_STANDARDIZATION S ON L.EVENT_CD = S.source_cv
		WHERE result_float IS NOT NULL;
	DROP TABLE REMAPe.ve3PhysioStr;
	CREATE TABLE REMAPe.ve3PhysioStr
		SELECT L.EVENT_ID, M.STUDYPATIENTID, L.event_utc, S.sub_standard_meaning, 
			REMAP.get_physio_result_str_epic(S.sub_standard_meaning, L.result_val) AS result_str, '' AS units, L.result_val AS documented_text
		FROM  
			(SELECT EVENT_ID, ENCNTR_ID, EVENT_CD, REMAP.to_utc(EVENT_END_DT_TM) AS event_utc, 
				RESULT_VAL, result_units_cd
			FROM REMAPe.tempv3Physio
			WHERE EVENT_CD IN (SELECT SOURCE_CV FROM COVID_SUPPLEMENT.CV_STANDARDIZATION WHERE source_table IN ('IP_FLWSHT_MEAS') 
				AND sub_standard_meaning IN ('Oxygen therapy delivery device', 'ECMO', 'Mode',  'Endotube Placement/Tube Status', 'Airway'))
			) AS L 
			JOIN REMAPe.ve3IdMap M ON L.ENCNTR_ID = M.ENCNTR_ID
			JOIN COVID_SUPPLEMENT.CV_STANDARDIZATION S ON L.EVENT_CD = S.source_cv
		WHERE RESULT_VAL IS NOT NULL;
DROP TABLE REMAPe.tempv3Physio; 

### no IO in Pinnacle ###

### pull relevant meds ###
DROP TABLE REMAPe.ve3Med;
CREATE TABLE REMAPe.ve3Med
	SELECT L.EVENT_ID, M.STUDYPATIENTID, REMAP.to_utc(MAR.TAKEN_TIME) AS event_utc, S.sub_standard_meaning, 
		MAR.INFUSION_RATE AS ADMIN_DOSAGE, U.abbr AS units, null AS route, S.source_display AS generic_name
	FROM 
		(SELECT DISTINCT ORDER_MED_ID AS EVENT_ID, PAT_ENC_CSN_ID AS FIN, MEDICATION_ID AS EVENT_CD, NULL AS event_utc
		FROM COVID_PINNACLE.ORDER_MEDINFO
		WHERE PAT_ENC_CSN_ID IN (SELECT FIN FROM REMAPe.ve3IdMap) 
			AND MEDICATION_ID IN (SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION WHERE source_table IN ('ORDER_MEDINFO'))
		) AS L 
		JOIN COVID_PINNACLE.MAR_ADMIN_INFO MAR ON L.EVENT_ID = MAR.ORDER_MED_ID
		JOIN REMAPe.ve3IdMap M ON L.FIN = M.FIN
		JOIN COVID_SUPPLEMENT.CV_STANDARDIZATION S ON L.EVENT_CD = S.source_cv
		JOIN COVID_PINNACLE.ZC_MED_UNIT U ON MAR.DOSE_UNIT_C = U.disp_qtyunit_c	
	WHERE MAR.INFUSION_RATE IS NOT NULL AND MAR.INFUSION_RATE > 0
;


### Create OrganSupportTnstance  ###
DROP TABLE REMAPe.ve3OrganSupportInstance;
	CREATE TABLE REMAPe.ve3OrganSupportInstance
	# VASO #
			SELECT event_id, studypatientid, event_utc, 'Vasopressor' AS support_type, NULL AS documented_source
			FROM REMAPe.ve3Med
			WHERE sub_standard_meaning = 'Vasopressor'
	   UNION  # HFNC #
			SELECT D.event_id, D.studypatientid, O.event_utc, 'HFNC' AS support_type, NULL AS documented_source
			FROM 
				(SELECT *
				 FROM REMAPe.ve3PhysioStr
				 WHERE sub_standard_meaning = 'Oxygen therapy delivery device' AND result_str = 'HFNC device'
				) AS D 
				JOIN (SELECT * 
				  FROM REMAPe.ve3Physio 
				  WHERE sub_standard_meaning = 'Oxygen Flow Rate' AND result_float >= 30
				) AS O ON (D.studypatientid = O.studypatientid AND TIMESTAMPDIFF(HOUR, D.event_utc, O.event_utc) BETWEEN -12 AND 12)
				JOIN (SELECT * 
				  FROM REMAPe.ve3Physio 
				  WHERE sub_standard_meaning = 'FiO2' AND result_float >= 40
				) AS F ON (O.studypatientid = F.studypatientid AND O.event_utc = F.event_utc)
		UNION # ECMO #
			SELECT event_id, studypatientid, event_utc, 'ECMO' AS support_type, NULL AS documented_source
			FROM REMAPe.ve3PhysioStr
			WHERE sub_standard_meaning = 'ECMO'
		UNION # NIV #
		SELECT event_id, NIV.studypatientid, event_utc, 'NIV' AS support_type, sub_standard_meaning AS documented_source
			FROM 
				(SELECT * 
				 FROM REMAPe.ve3PhysioStr	
				 WHERE (sub_standard_meaning = 'Mode' AND result_str = 'NIV mode')
					OR (sub_standard_meaning = 'Oxygen therapy delivery device' AND result_str = 'NIV device')
				 ) AS NIV 
			LEFT JOIN REMAP.NIVexclusion E ON NIV.StudyPatientID = E.StudyPatientID
			WHERE E.StudyPatientID IS NULL OR NOT event_utc BETWEEN NIV_exclusion_start_utc AND NIV_exclusion_end_utc
		UNION # IMV #
			SELECT DISTINCT event_id, studypatientid, event_utc, 'IMV' AS support_type, documented_source
			FROM
			  (SELECT *, 'Oxygen therapy delivery device' AS documented_source
				FROM REMAPe.ve3PhysioStr
				WHERE sub_standard_meaning = 'Oxygen therapy delivery device' AND result_str = 'IV device'
			  UNION
				SELECT *, 'Endotube placement' AS documented_source
				FROM REMAPe.ve3PhysioStr
				WHERE sub_standard_meaning = 'Endotube Placement/Tube Status'  # may need split. And definitions added to get_physio_result_str_epic()
			  UNION			
				SELECT *, 'Airway' AS documented_source
				FROM REMAPe.ve3PhysioStr
				WHERE sub_standard_meaning = 'Airway' #AND result_str = 'Endotracheal'
			  UNION			
				SELECT *, 'Mode' AS documented_source
				FROM REMAPe.ve3PhysioStr
				WHERE sub_standard_meaning = 'Mode' AND result_str = 'IV mode'
			) AS IMV
;
	
### find severe randomization times ###
DROP TABLE REMAPe.ve3RandomizedSevere;
CREATE TABLE REMAPe.ve3RandomizedSevere
	WITH screening_loc AS (
		SELECT STUDYPATIENTID, loc_order 
		FROM REMAPe.ve3LocOrder 
		WHERE screening_location = 1
	)
	SELECT P.STUDYPATIENTID, GREATEST(P.screendate_utc, earliest_ICU_organ_support_utc) AS randomized_utc
	FROM REMAPe.ve3Participant P
	JOIN (
		SELECT L.studypatientid, GREATEST(MIN(O.event_utc), MIN(L.beg_utc)) AS earliest_ICU_organ_support_utc
		FROM REMAPe.ve3LocOrder L
		JOIN screening_loc S ON L.studypatientid = S.studypatientid
		JOIN REMAPe.ve3OrganSupportInstance O ON L.studypatientid = O.studypatientid
		LEFT JOIN COVID_PINNACLE.CLARITY_DEP U ON L.LOC_NURSE_UNIT_CD = U.department_id
		WHERE O.event_utc BETWEEN ADDDATE(L.beg_utc, INTERVAL -12 HOUR) AND L.end_utc
			AND specialty IN ('Intensive Care')
			AND L.loc_order >= S.loc_order
		GROUP BY L.studypatientid
	) AS O ON P.studypatientid = O.studypatientid
;

### find moderate randomization times ###
DROP TABLE REMAPe.ve3RandomizedModerate;
CREATE TABLE REMAPe.ve3RandomizedModerate
	SELECT P.STUDYPATIENTID, P.screendate_utc as randomized_utc
	FROM REMAPe.ve3Participant P
	LEFT JOIN REMAPe.ve3RandomizedSevere R ON P.studypatientid = R.studypatientid
	WHERE R.randomized_utc IS NULL OR R.randomized_utc > P.screendate_utc
;

### define study days within the first 30 ###
DROP TABLE REMAPe.ve3StudyDay;
	CREATE TABLE REMAPe.ve3StudyDay
		SELECT 
			STUDYPATIENTID, STUDY_DAY,
			day_date_local, day_start_utc, day_end_utc,
			RandomizationType
		FROM 
			(SELECT 
				P.StudyPatientID, P.randomized_utc, P.RandomizationType, P.STUDY_DAY,
				CASE
					WHEN P.STUDY_DAY < 1 THEN DATE_ADD(P.randomized_date_local, INTERVAL P.STUDY_DAY DAY) 
					ELSE DATE_ADD(P.randomized_date_local, INTERVAL P.STUDY_DAY-1 DAY) 
				END AS day_date_local,
				CASE  
					WHEN P.STUDY_DAY = 1 THEN P.randomized_utc
					WHEN P.STUDY_DAY < 1 THEN REMAP.to_datetime_utc(DATE_ADD(P.randomized_date_local, INTERVAL P.STUDY_DAY DAY)) 
					ELSE REMAP.to_datetime_utc(DATE_ADD(P.randomized_date_local, INTERVAL P.STUDY_DAY-1 DAY)) 
				END AS day_start_utc,
				CASE
					WHEN P.STUDY_DAY = 0 THEN P.randomized_utc
					WHEN P.STUDY_DAY < 0 THEN REMAP.to_datetime_utc(DATE_ADD(P.randomized_date_local, INTERVAL P.STUDY_DAY+1 DAY)) 
					ELSE REMAP.to_datetime_utc(DATE_ADD(P.randomized_date_local, INTERVAL P.STUDY_DAY DAY)) 
				END AS day_end_utc,
				L.EndOfHospitalization_utc  
			FROM 
				( SELECT studypatientID, randomized_utc, DATE(REMAP.to_local(randomized_utc)) as randomized_date_local,
					'Moderate' AS RandomizationType, study_day
					 FROM REMAPe.ve3RandomizedModerate
					 JOIN COVID_SUPPLEMENT.STUDY_DAY ON 1 = 1
				 UNION
				  SELECT studypatientID, randomized_utc, DATE(REMAP.to_local(randomized_utc)) as randomized_date_local, 
				 	'Severe' AS RandomizationType, study_day
					 FROM REMAPe.ve3RandomizedSevere
					 JOIN COVID_SUPPLEMENT.STUDY_DAY ON 1 = 1
				 ) AS P
				JOIN (SELECT studypatientid, if(MAX(end_utc) = '2100-12-31 00:00:00', NULL, MAX(end_utc)) AS EndOfHospitalization_utc
				 FROM REMAPe.ve3LocOrder GROUP BY STUDYPATIENTID) AS L ON P.studypatientid = L.studypatientid
			) AS temp_result
		WHERE
			(EndOfHospitalization_utc IS NULL 
				OR
			day_start_utc < EndOfHospitalization_utc)
				AND 
			day_end_utc < CURRENT_TIMESTAMP 
		ORDER BY STUDYPATIENTID, STUDY_DAY, RandomizationType
;


### REMAP.v3eUnitStay ###
DROP TABLE REMAPe.ve3UnitStay;
	CREATE TABLE REMAPe.v3tempDefinedLocOrder 
		SELECT L1.STUDYPATIENTID, L1.encntr_id, L1.beg_utc, L1.end_utc, L1.loc_order, 
			L1.LOC_NURSE_UNIT_CD, 
			REMAP.convert_epic_unit(U.specialty) AS unit_type, 
			U.department_name AS display, L2.includes_organSupport, 
			0 AS includes_stepdownUnit
		FROM REMAPe.ve3LocOrder L1
		LEFT JOIN (
			SELECT DISTINCT L.studypatientid, L.loc_order, 1 AS includes_organSupport 
			FROM REMAPe.ve3LocOrder L
			JOIN REMAPe.ve3OrganSupportInstance O ON L.studypatientid = O.studypatientid
			WHERE O.event_utc BETWEEN L.beg_utc AND L.end_utc
			) AS L2 ON L1.studypatientid = L2.studypatientid AND L1.loc_order = L2.loc_order
		LEFT JOIN COVID_PINNACLE.CLARITY_DEP U ON L1.LOC_NURSE_UNIT_CD = U.department_id
	;
	## find contiguous unit stays ##
	CREATE TABLE REMAPe.ve3UnitStay
		WITH unitTransition AS (
				SELECT D1.studypatientid, D1.beg_utc, D1.end_utc, D1.loc_order, D1.unit_type, D2.unit_type AS next_unit 
				FROM REMAPe.v3tempDefinedLocOrder D1 
				JOIN REMAPe.v3tempDefinedLocOrder D2 
					ON D1.studypatientid = D2.studypatientid AND D1.loc_order = D2.loc_order-1
				WHERE D1.unit_type <> D2.unit_type OR D1.unit_type IS NULL OR D2.unit_type IS NULL 
			), unitEndpoints AS (# find connections between unit transitions 
				SELECT u1.studypatientid, MAX(u2.loc_order)+1 AS loc_order_unit_start, u1.loc_order AS loc_order_unit_end  
				FROM unitTransition u1 JOIN unitTransition u2 ON u1.studypatientid = u2.studypatientid 
				WHERE u1.loc_order > u2.loc_order
				GROUP BY u1.studypatientid, u1.loc_order
			), minLocOrder AS ( # towards first (b/c first unit does not get captured in unitEndpoints)
				SELECT studypatientid, MIN(loc_order) AS min_loc_order 
				FROM REMAPe.v3tempDefinedLocOrder GROUP BY studypatientid
			), firstUnit AS ( # towards first
				SELECT D.studypatientid, D.loc_order 
				FROM REMAPe.v3tempDefinedLocOrder D 
				JOIN minLocOrder m ON D.studypatientid = m.studypatientid
				WHERE D.loc_order = m.min_loc_order
			), firstTransition AS ( # towards first
				SELECT studypatientid, MIN(loc_order) AS loc_order 
				FROM unitTransition 
				GROUP BY studypatientid
			), unitEndpointsFirst AS ( # is first
				SELECT u1.studypatientid, u1.loc_order AS loc_order_unit_start, u2.loc_order AS loc_order_unit_end 
				FROM firstUnit u1 
				JOIN firstTransition u2 ON u1.studypatientid = u2.studypatientid 
			), maxLocOrder AS ( # towards last (b/c last unit does not get captured in unitEndpoints)
				SELECT studypatientid, MAX(loc_order) AS max_loc_order 
				FROM REMAPe.v3tempDefinedLocOrder 
				GROUP BY studypatientid
			), lastUnit AS ( # towards last
				SELECT D.studypatientid, D.loc_order 
				FROM REMAPe.v3tempDefinedLocOrder D 
				JOIN maxLocOrder m ON D.studypatientid = m.studypatientid
				WHERE D.loc_order = m.max_loc_order
			), lastTransition AS ( # towards last
				SELECT studypatientid, MAX(loc_order)+1 AS loc_order 
				FROM unitTransition 
				GROUP BY studypatientid 
			), unitEndpointsLast AS ( # is last
				SELECT u1.studypatientid, u2.loc_order AS loc_order_unit_start, u1.loc_order AS loc_order_unit_end 
				FROM lastUnit u1 
				JOIN lastTransition u2 ON u1.studypatientid = u2.studypatientid 
			), unitEndpointsAll AS (
				SELECT *, 'first' AS q FROM unitEndpointsFirst
				UNION SELECT *, 'tran' AS q FROM unitEndpoints
				UNION SELECT *, 'last' AS q FROM unitEndpointsLast
			), organSupportLoc AS ( 
				SELECT studypatientid, loc_order, includes_organSupport 
				FROM REMAPe.v3tempDefinedLocOrder 
				WHERE includes_organSupport = 1
			)
			SELECT DISTINCT E.STUDYPATIENTID, Ds.unit_type, E.loc_order_unit_start, E.loc_order_unit_end, Ds.beg_utc, De.end_utc, 
				ifnull(I.includes_organSupport, 0) AS includes_organSupport, 0 AS includes_stepdownUnit, 
				if(Ds.unit_type IN ('ignore'), 1, 0) AS includes_ignoreUnit 
			FROM unitEndpointsAll E
			JOIN REMAPe.v3tempDefinedLocOrder Ds ON E.studypatientid = Ds.studypatientid AND E.loc_order_unit_start = Ds.loc_order
			JOIN REMAPe.v3tempDefinedLocOrder De ON E.studypatientid = De.studypatientid AND E.loc_order_unit_end = De.loc_order
			LEFT JOIN organSupportLoc I ON E.studypatientid = I.studypatientid 
				AND I.loc_order BETWEEN E.loc_order_unit_start AND E.loc_order_unit_end
			WHERE Ds.unit_type = De.unit_type  # where statement is need because in rare cases a pt is as two locs that start at the same time
			ORDER BY StudyPatientID, loc_order_unit_start;
	## add stepdownUnit flag ##	
	UPDATE REMAPe.ve3UnitStay U
	JOIN (
		SELECT * 
		FROM REMAPe.v3tempDefinedLocOrder 
		WHERE includes_stepdownUnit = 1
	) AS L ON U.studypatientid = L.studypatientid AND L.loc_order BETWEEN U.loc_order_unit_start AND U.loc_order_unit_end
	SET U.includes_stepdownUnit = L.includes_stepdownUnit;
	## drop temp table ##
	DROP TABLE REMAPe.v3tempDefinedLocOrder;


### identify ICU stays ###
DROP TABLE REMAPe.ve3IcuStay; 
CREATE TABLE REMAPe.ve3IcuStay
	WITH stay_before AS (  # Unit stays of type ICU that have another unit stay of type ICU within the 12 hours before
		SELECT U1.*, U2.loc_order_unit_start AS linked_start
		FROM REMAPe.ve3UnitStay U1
		JOIN REMAPe.ve3UnitStay U2
		ON U1.studypatientid = U2.studypatientid AND U1.beg_utc BETWEEN U2.end_utc AND ADDDATE(U2.end_utc, INTERVAL 12 HOUR)
		WHERE U1.unit_type IN ('ICU') AND U2.unit_type IN ('ICU')
		), stay_after AS ( # Unit stays of type ICU that have another unit stay of type ICU within the 12 hours after
		SELECT U1.*, U2.loc_order_unit_start AS linked_end
		FROM REMAPe.ve3UnitStay U1
		JOIN REMAPe.ve3UnitStay U2
		ON U1.studypatientid = U2.studypatientid AND U1.end_utc BETWEEN ADDDATE(U2.beg_utc, INTERVAL -12 HOUR) AND U2.beg_utc
		WHERE U1.unit_type IN ('ICU') AND U2.unit_type IN ('ICU')	
		), stay_joint AS (  # joining before and after stays with each stay
		SELECT U1.*, SB.linked_start, SA.linked_end 
		FROM REMAPe.ve3UnitStay U1
		LEFT JOIN stay_before SB ON U1.STUDYPATIENTID = SB.studypatientid AND U1.loc_order_unit_start = SB.loc_order_unit_start
		LEFT JOIN stay_after SA ON U1.STUDYPATIENTID = SA.studypatientid AND U1.loc_order_unit_start = SA.loc_order_unit_start
		WHERE U1.unit_type IN ('ICU') 
		), independent_rows AS (  # unit stay of type icu that does not have a neighboring stay of type icu
		SELECT *
		FROM stay_joint 
		WHERE linked_start IS NULL AND linked_end IS NULL
		), start_rows AS ( # unit stay of type icu that are the start of neighboring stays of type icu
		SELECT *
		FROM stay_joint 
		WHERE linked_start IS NULL AND linked_end IS NOT NULL
		), end_rows AS ( # unit stay of type icu that are the end of neighboring stays of type icu
		SELECT *
		FROM stay_joint 
		WHERE linked_start IS NOT NULL AND linked_end IS NULL
		), joined_rows AS (  # join the start and end neighboring types and calculate the difference 
		SELECT 
			SR.studypatientid, SR.unit_type, SR.loc_order_unit_start, ER.loc_order_unit_end,
			SR.beg_utc, ER.end_utc, GREATEST(SR.includes_organSupport, ER.includes_organSupport) AS includes_organSupport,
			GREATEST(SR.includes_stepdownUnit, ER.includes_stepdownUnit) AS includes_stepdownUnit,
			GREATEST(SR.includes_ignoreUnit, ER.includes_ignoreUnit) AS includes_ignoreUnit,
			TIMESTAMPDIFF(SECOND, SR.beg_utc, ER.end_utc) AS time_diff 
		FROM start_rows SR 
		JOIN end_rows ER 
		ON SR.studypatientid = ER.studypatientid
		WHERE SR.beg_utc <= ER.beg_utc
		), min_joined AS (  # find the minimum differece between nighboring types incase a pt has multiple
		SELECT studypatientid, loc_order_unit_start, MIN(time_diff) AS min_time_diff
		FROM joined_rows 
		GROUP BY studypatientid, loc_order_unit_start
		) # union the closest neighboring types with the independent types 	
			SELECT 
				REMAP.to_unique_orderd_num(J.beg_utc, J.studypatientid) AS upk,
				J.StudyPatientId, 0 AS stay_count, J.beg_utc, J.end_utc, J.includes_organSupport, J.includes_stepdownUnit, J.includes_ignoreUnit,
				J.loc_order_unit_start AS loc_start, J.loc_order_unit_end AS loc_end 
			FROM joined_rows J
			JOIN min_joined M
			ON J.studypatientid = M.studypatientid AND J.loc_order_unit_start = M.loc_order_unit_start
			WHERE J.time_diff = M.min_time_diff
		UNION 
			SELECT 
				REMAP.to_unique_orderd_num(beg_utc, studypatientid) AS upk,
				StudyPatientId, 0 AS stay_count, beg_utc, end_utc, includes_organSupport, includes_stepdownUnit, includes_ignoreUnit,
				loc_order_unit_start AS loc_start, loc_order_unit_end AS loc_end
			FROM independent_rows
		ORDER BY studypatientid, beg_utc
		;
	## adjust upks to be sequential and start at 0 ##
	UPDATE REMAPe.ve3IcuStay S
		JOIN (
				SELECT 0 AS new_upk, MIN(upk) AS upk 
				FROM REMAPe.ve3IcuStay
			UNION
				SELECT COUNT(*) AS new_upk, S1.upk 
				FROM REMAPe.ve3IcuStay S1 
				JOIN REMAPe.ve3IcuStay S2 ON 1=1
				WHERE S1.upk > S2.upk
				GROUP BY S1.upk
		) AS new_pk ON S.upk = new_pk.upk
		SET S.upk = new_pk.new_upk
		WHERE S.upk = new_pk.upk
	;
	## set stay count column ##
	UPDATE REMAPe.ve3IcuStay S
		JOIN (
			SELECT COUNT(*) AS stay_count, S1.StudyPatientID, S1.upk 
			FROM REMAPe.ve3IcuStay S1 
			JOIN REMAPe.ve3IcuStay S2 ON S1.STUDYPATIENTID = S2.Studypatientid 
			WHERE S1.upk > S2.upk
			GROUP BY S1.StudyPatientID, S1.upk
		) AS stay_count ON S.upk = stay_count.upk
		SET S.stay_count = stay_count.stay_count		
;

SELECT * FROM REMAPe.ve3OrganSupportInstance LIMIT 1000;
SELECT * FROM REMAPe.ve3RandomizedSevere;
SELECT * FROM REMAPe.ve3RandomizedModerate;
SELECT * FROM REMAPe.ve3StudyDay;
SELECT * FROM REMAPe.ve3UnitStay;
SELECT * FROM REMAPe.ve3IcuStay;


/* **************************** */

### ve3RRTInstance ####
DROP TABLE REMAPe.ve3RRTInstance;
	CREATE TABLE REMAPe.ve3RRTInstance
		SELECT DISTINCT event_id, studypatientid, event_utc, 'RRT' AS support_type, documented_source
			FROM
			  (SELECT *, 'Physio' AS documented_source
				FROM REMAPe.ve3Physio
				WHERE sub_standard_meaning = 'RRT'
			) AS RRT
;	

DROP TABLE REMAPe.ve3SupplementalOxygenInstance;
	CREATE TABLE REMAPe.ve3SupplementalOxygenInstance
		SELECT device.event_id, device.STUDYPATIENTID, device.event_utc, device.support_type
		FROM 
			(
			SELECT event_id, STUDYPATIENTID, event_utc, result_str AS support_type FROM REMAPe.ve3PhysioStr 
				WHERE sub_standard_meaning = 'Oxygen therapy delivery device' 
				AND result_str IN ('HFNC device', 'NC', 'Mask', 'Nonrebreather', 'Prebreather')	
			) AS device
			JOIN (		
				SELECT STUDYPATIENTID, event_utc, result_float AS Oxygen_Flow_Rate, units
				FROM REMAPe.ve3Physio WHERE sub_standard_meaning = 'Oxygen Flow Rate'
			) AS O2 ON (device.studypatientid = O2.studypatientid 
				AND TIMESTAMPDIFF(HOUR, device.event_utc, O2.event_utc) BETWEEN -12 AND 12)
		WHERE device.event_id NOT IN (SELECT event_id FROM REMAPe.ve3OrganSupportInstance where support_type = 'HFNC')
		UNION
		SELECT 
			D.event_id, D.studypatientid, O.event_utc, 'relaxedHF' AS support_type
		FROM 
			(SELECT *
			 FROM REMAPe.ve3PhysioStr
			 WHERE sub_standard_meaning = 'Oxygen therapy delivery device' AND result_str = 'HFNC device'
			) AS D 
			JOIN (SELECT * 
			  FROM REMAPe.ve3Physio 
			  WHERE sub_standard_meaning = 'Oxygen Flow Rate' AND result_float >= 20
			) AS O ON (D.studypatientid = O.studypatientid AND TIMESTAMPDIFF(HOUR, D.event_utc, O.event_utc) BETWEEN -12 AND 12)
			JOIN (SELECT * 
			  FROM REMAPe.ve3Physio 
			  WHERE sub_standard_meaning = 'FiO2' AND result_float >= 21
			) AS F ON (O.studypatientid = F.studypatientid AND O.event_utc = F.event_utc)
; 

### ve3CalculatedHourlyFiO2 ###
DROP TABLE REMAPe.ve3CalculatedHourlyFiO2;
	CREATE TABLE REMAPe.ve3CalculatedHourlyFiO2 
	WITH TempOxygenationDevices AS (
		SELECT 
			device.STUDYPATIENTID, device.event_utc, device.support_type, Oxygen_Flow_Rate, units
		FROM	
			( # HFNC from organ support instance table removed from query b/c FiO2 will be documented in this case
			SELECT event_id, STUDYPATIENTID, event_utc, result_str AS support_type, '' AS documented_source 
			FROM REMAPe.ve3PhysioStr 
				WHERE sub_standard_meaning = 'Oxygen therapy delivery device' 
				AND result_str IN ('HFNC device', 'NC', 'Mask', 'Nonrebreather', 'Prebreather')	
			) AS device
			JOIN (		
				SELECT STUDYPATIENTID, event_utc, result_float AS Oxygen_Flow_Rate, units
				FROM REMAPe.ve3Physio WHERE sub_standard_meaning = 'Oxygen Flow Rate'
			) AS O2 ON (device.studypatientid = O2.studypatientid AND device.event_utc = O2.event_utc)
	), CalcFiO2 AS (
		SELECT * FROM
			(SELECT 
				StudyPatientId, event_utc, support_type, 
				CASE
					WHEN support_type = 'NC' THEN 
						CASE 
							WHEN Oxygen_Flow_Rate <= 1 THEN 24
							WHEN Oxygen_Flow_Rate <= 2 THEN 28
							WHEN Oxygen_Flow_Rate <= 3 THEN 32
							WHEN Oxygen_Flow_Rate <= 4 THEN 36
							WHEN Oxygen_Flow_Rate <= 5 THEN 40
							WHEN Oxygen_Flow_Rate <= 6 THEN 44
							WHEN Oxygen_Flow_Rate <= 7 THEN 48
							WHEN Oxygen_Flow_Rate <= 8 THEN 52
							WHEN Oxygen_Flow_Rate <= 9 THEN 56
							WHEN Oxygen_Flow_Rate <= 10 THEN 60
							ELSE NULL
						END
					WHEN support_type = 'Mask' THEN
						CASE 
							WHEN Oxygen_Flow_Rate BETWEEN 8 AND 15 THEN 70
							ELSE NULL
						END
					WHEN support_type = 'Nonrebreather' THEN
						CASE 
							WHEN Oxygen_Flow_Rate BETWEEN 8 AND 15 THEN 95
							WHEN Oxygen_Flow_Rate > 15 THEN 99
							ELSE NULL
						END
					WHEN support_type = 'Prebreather' THEN
						CASE 
							WHEN Oxygen_Flow_Rate BETWEEN 8 AND 15 THEN 70
							ELSE NULL
						END
					WHEN support_type = 'HFNC device' THEN
						CASE 
							WHEN Oxygen_Flow_Rate BETWEEN 8 AND 15 THEN 70
							WHEN Oxygen_Flow_Rate > 15 THEN 100
							ELSE NULL
						END
					ELSE NULL
				END AS result_float
			FROM
				TempOxygenationDevices
			) AS IR
		WHERE 
			result_float IS NOT NULL
	), joined_fio2 AS (
		SELECT STUDYPATIENTID, MAX(result_float) AS result_float, priority, MIN(event_utc) AS event_utc, unix_hour 
		FROM 
		 (SELECT STUDYPATIENTID, event_utc, result_float, 0 AS priority, FLOOR(UNIX_TIMESTAMP(event_utc)/(60*60)) AS unix_hour  
		 FROM REMAPe.ve3Physio 
		 WHERE sub_standard_meaning = 'FiO2' AND result_float BETWEEN 21 AND 100
		UNION
		 SELECT STUDYPATIENTID, event_utc, result_float, 1 AS priority, FLOOR(UNIX_TIMESTAMP(event_utc)/(60*60)) AS unix_hour 
		 FROM CalcFiO2
		 ) AS F
		GROUP BY STUDYPATIENTID, priority, unix_hour 
	) 
	SELECT J.STUDYPATIENTID, J.event_utc, J.result_float, if(priority=0, 'documented', 'calculated') as fio2_source  
	FROM joined_fio2 J
	JOIN (SELECT STUDYPATIENTID, MIN(priority) AS min_priority, unix_hour FROM joined_fio2 GROUP BY STUDYPATIENTID, unix_hour
	) M ON (J.STUDYPATIENTID = M.STUDYPATIENTID AND J.unix_hour = M.unix_hour)
	WHERE J.priority = M.min_priority
;

# REMAPe.ve3calculatedPFratio
DROP TABLE REMAPe.ve3CalculatedPFratio;
	CREATE TABLE REMAPe.tempPnearestF
		WITH PjoinF AS (
			SELECT P.STUDYPATIENTID, P.result_float as PaO2_float, P.event_utc AS PaO2_utc, 
				F.result_float AS FiO2_float,
				ifnull(F.event_utc, ADDDATE(P.event_utc, INTERVAL -1439 MINUTE)) AS FiO2_utc,  # handles cases where PaO2 is reported without an FIO2
				TIMESTAMPDIFF(MINUTE, ifnull(F.event_utc, ADDDATE(P.event_utc, INTERVAL -1439 MINUTE)), P.event_utc) AS delta_minutes
			FROM
				(SELECT * FROM REMAPe.ve3Lab WHERE sub_standard_meaning IN ('PaO2')) AS P
				LEFT JOIN (SELECT * FROM REMAPe.ve3CalculatedHourlyFiO2) AS F 
				ON P.STUDYPATIENTID = F.STUDYPATIENTID AND F.event_utc BETWEEN ADDDATE(P.event_utc, INTERVAL -24 HOUR) AND P.event_utc
		), minFs AS (
			SELECT STUDYPATIENTID, PaO2_utc, MIN(delta_minutes) AS min_delta FROM PjoinF GROUP BY STUDYPATIENTID, PaO2_utc
		)
		SELECT PF.* FROM PjoinF PF JOIN minFs F ON PF.STUDYPATIENTID = F.STUDYPATIENTID AND PF.PaO2_utc = F.PaO2_utc 
		WHERE PF.delta_minutes = F.min_delta
	;
	CREATE TABLE REMAPe.tempPnearestE	
		WITH PjoinE AS (
			SELECT P.STUDYPATIENTID, P.result_float as PaO2_float, P.event_utc AS PaO2_utc, 
				E.result_float AS PEEP_float, E.event_utc AS PEEP_utc, TIMESTAMPDIFF(MINUTE, E.event_utc, P.event_utc) AS delta_minutes
			FROM
				(SELECT * FROM REMAPe.ve3Lab WHERE sub_standard_meaning IN ('PaO2')) AS P
				JOIN (SELECT * FROM REMAPe.ve3Physio WHERE sub_standard_meaning IN ('PEEP')) AS E 
				ON P.STUDYPATIENTID = E.STUDYPATIENTID AND E.event_utc BETWEEN ADDDATE(P.event_utc, INTERVAL -24 HOUR) AND P.event_utc	
		), minEs AS (
			SELECT STUDYPATIENTID, PaO2_utc, MIN(delta_minutes) AS min_delta FROM PjoinE GROUP BY STUDYPATIENTID, PaO2_utc
		)
		SELECT PE.* FROM PjoinE PE JOIN minEs E ON PE.STUDYPATIENTID = E.STUDYPATIENTID AND PE.PaO2_utc = E.PaO2_utc 
		WHERE PE.delta_minutes = E.min_delta
	;	
	CREATE TABLE REMAPe.ve3CalculatedPFratio
		SELECT DISTINCT PF.STUDYPATIENTID, ROUND(PF.PaO2_float/PF.FiO2_float*100,0) AS PF_ratio, 
			PF.PaO2_float, PF.PaO2_utc, PF.FiO2_float, PF.FiO2_utc, PE.PEEP_float, PE.PEEP_utc 
		FROM REMAPe.tempPnearestF PF 
		LEFT JOIN REMAPe.tempPnearestE PE ON PF.STUDYPATIENTID = PE.STUDYPATIENTID AND PF.PaO2_utc = PE.PaO2_utc
 	;
 	DROP TABLE REMAPe.tempPnearestF;
	DROP TABLE REMAPe.tempPnearestE
; 


### v3CalculatedPEEPjoinFiO2 ###			
DROP TABLE REMAPe.ve3CalculatedPEEPjoinFiO2;
CREATE TABLE REMAPe.ve3CalculatedPEEPjoinFiO2
	WITH PEEPjoinFiO2 AS (
		SELECT P.STUDYPATIENTID, P.result_float as PEEP_float, P.event_utc AS PEEP_utc, 
			F.result_float AS FiO2_float, F.event_utc AS FiO2_utc, TIMESTAMPDIFF(MINUTE, F.event_utc, P.event_utc) AS delta_minutes
		FROM
			(SELECT * FROM REMAPe.ve3Physio WHERE sub_standard_meaning IN ('PEEP')) AS P
			JOIN REMAPe.ve3CalculatedHourlyFiO2 AS F 
			ON P.STUDYPATIENTID = F.STUDYPATIENTID 
				AND F.event_utc BETWEEN ADDDATE(P.event_utc, INTERVAL -4 HOUR) AND ADDDATE(P.event_utc, INTERVAL 4 HOUR)
	), minFs AS (
		SELECT STUDYPATIENTID, PEEP_utc, MIN(delta_minutes) AS min_delta FROM PEEPjoinFiO2 GROUP BY STUDYPATIENTID, PEEP_utc
	)
	SELECT 
		PF.StudyPatientID, PF.FiO2_float, PF.FiO2_utc, PF.PEEP_float, PF.PEEP_utc
	FROM PEEPjoinFiO2 PF 
	JOIN minFs F ON PF.STUDYPATIENTID = F.STUDYPATIENTID AND PF.PEEP_utc = F.PEEP_utc 
	WHERE PF.delta_minutes = F.min_delta
;

### STATE HYPOXIA ### 
DROP TABLE REMAPe.ve3CalculatedStateHypoxiaAtEnroll;
	CREATE TABLE REMAPe.ve3CalculatedStateHypoxiaAtEnroll
	WITH PFmoderate AS (
			SELECT PF.* FROM REMAPe.ve3CalculatedPFratio PF JOIN REMAPe.ve3RandomizedModerate R ON PF.STUDYPATIENTID = R.STUDYPATIENTID
			WHERE PF.PaO2_utc <= R.randomized_utc
		), minModerate AS (
			SELECT STUDYPATIENTID, MAX(PaO2_utc) AS max_PaO2_utc FROM PFmoderate GROUP BY STUDYPATIENTID 
		), PFsevere AS (
			SELECT PF.* FROM REMAPe.ve3CalculatedPFratio PF JOIN REMAPe.ve3RandomizedSevere R ON PF.STUDYPATIENTID = R.STUDYPATIENTID
			WHERE PF.PaO2_utc <= R.randomized_utc
		), minSevere AS (
			SELECT STUDYPATIENTID, MAX(PaO2_utc) AS max_PaO2_utc FROM PFsevere GROUP BY STUDYPATIENTID 
		), IMV AS ( # by definition, IMV is only possible for Severe state
			SELECT DISTINCT I.STUDYPATIENTID, 1 AS OnInvasiveVent 
			FROM 
				(SELECT STUDYPATIENTID, event_utc FROM REMAPe.ve3OrganSupportInstance WHERE support_type = 'IMV') AS I
				JOIN REMAPe.ve3RandomizedSevere R ON (I.STUDYPATIENTID = R.STUDYPATIENTID)
			WHERE I.event_utc <= R.randomized_utc
		), allJoined AS (
			SELECT PF.*, 'Moderate' AS RandomizationType, 0 AS OnInvasiveVent FROM PFmoderate PF 
				JOIN minModerate M ON PF.STUDYPATIENTID = M.STUDYPATIENTID AND PF.PaO2_utc = M.max_PaO2_utc 
			UNION 
			SELECT PF.*, 'Severe' AS RandomizationType, if(IMV.OnInvasiveVent IS NULL, 0, 1) FROM PFsevere PF 
				JOIN minSevere M ON PF.STUDYPATIENTID = M.STUDYPATIENTID AND PF.PaO2_utc = M.max_PaO2_utc
				LEFT JOIN IMV ON PF.STUDYPATIENTID = IMV.STUDYPATIENTID  
		), IMVwithoutPaO2 AS (
			SELECT *
			FROM IMV
			WHERE studypatientid NOT IN (SELECT DISTINCT studypatientid FROM allJoined WHERE RandomizationType = 'Severe')
				AND studypatientid NOT IN (SELECT DISTINCT studypatientid FROM REMAP.v3RandomizedModerate)
		), allJoinedPlus AS (
			SELECT * FROM allJoined
			UNION
			SELECT 
				StudyPatientID, NULL AS PF_ratio, NULL AS PaO2_float, NULL AS PaO2_utc,
				NULL AS FiO2_float, NULL AS FiO2_utc, NULL AS PEEP_float, NULL AS PEEP_utc,  
				'Severe' AS RandomizationType, onInvasiveVent
			FROM IMVwithoutPaO2
		)
		SELECT A.StudyPatientID, A.RandomizationType,
			CASE 
				WHEN A.OnInvasiveVent = 1 AND A.PaO2_float IS NOT NULL AND A.PEEP_float IS NOT NULL AND A.PF_ratio IS NOT NULL 
					THEN if(A.PEEP_float >= 5 AND A.PF_ratio < 200, 3, 2)
				WHEN A.OnInvasiveVent = 1 THEN 2
				ELSE 1
			END AS StateHypoxia,
			A.onInvasiveVent, A.PaO2_float, A.PaO2_utc, A.PEEP_float, A.PEEP_utc, A.PF_ratio, A.FiO2_float, A.FiO2_utc 
		FROM allJoinedPlus A
;


### v3CalculatedSOFA ###
DROP TABLE REMAPe.ve3CalculatedSOFA;	
	CREATE TABLE REMAPe.ve3CalculatedSOFA
		## MAP
		SELECT CEP.StudyPatientId, SD.study_day, 1 AS score, RandomizationType
		FROM (
			SELECT StudyPatientID, event_utc, result_float
			FROM REMAPe.ve3Physio 
			WHERE sub_standard_meaning IN ('Blood Pressure (MAP)') AND result_float < 70
		) AS CEP	
		JOIN REMAPe.ve3StudyDay SD 
			ON (CEP.StudyPatientId = SD.StudyPatientId AND event_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		UNION
		## MAP-calculated 	
		SELECT StudyPatientId, study_day, 1 AS score, RandomizationType
		FROM (
			SELECT D.STUDYPATIENTID, SD.study_day, (D.result_float * 2 + S.result_float)/3 AS result_float, RandomizationType
			FROM REMAPe.ve3Physio AS D
			JOIN REMAPe.ve3Physio AS S ON (D.StudyPatientID = S.StudyPatientID AND D.event_utc = S.event_utc)			
			JOIN REMAPe.ve3StudyDay SD 
				ON (D.StudyPatientId = SD.StudyPatientId AND D.event_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
			WHERE D.sub_standard_meaning IN ('Blood pressure (arterial diastolic)') 
				AND S.sub_standard_meaning IN ('Blood pressure (arterial systolic)')
		) AS calculated_MAP
		WHERE result_float < 70
		# vaso-based
		UNION
		SELECT StudyPatientId, study_day, score, RandomizationType
		FROM (
			WITH med AS (
				SELECT M.StudyPatientID, M.event_utc AS med_utc, M.admin_dosage, M.units, M.route, 
					generic_name as display, study_day, RandomizationType
				FROM REMAPe.ve3Med M
				JOIN REMAPe.ve3StudyDay SD ON (M.StudyPatientId = SD.StudyPatientId 
					AND event_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
				WHERE MINUTE(event_utc) = 0
			),	weight AS (
				SELECT StudyPatientID, event_utc AS weight_utc, result_float AS weight, units AS weight_units
				FROM REMAPe.ve3Physio
				WHERE sub_standard_meaning = 'Weight (kg)'
			), joined AS (
				SELECT med.*, weight, weight_units, weight_utc,
					admin_dosage*1000/weight/60 AS converted_dose, 'mcg/kg/min' AS converted_units,
					TIMESTAMPDIFF(MINUTE, weight_utc, med_utc) AS minutes_since_weight 
				FROM med 
				JOIN weight ON (med.StudyPatientID = weight.StudyPatientID AND med_utc >= weight_utc)
			), min_point AS (	
				SELECT StudyPatientID, med_utc, RandomizationType, MIN(minutes_since_weight) AS min_MSW 
				FROM joined 
				GROUP BY StudyPatientID, med_utc, RandomizationType
			), keeper_meds AS (
				SELECT joined.*
				FROM joined 
				JOIN min_point ON (joined.StudyPatientID = min_point.StudyPatientID 
					AND joined.med_utc = min_point.med_utc 
					AND joined.RandomizationType = min_point.RandomizationType
					AND joined.minutes_since_weight = min_point.min_MSW)
			)
			SELECT StudyPatientID, study_day, 
				REMAP.convert_vaso_to_sofa_points(display, converted_dose) AS score,
				RandomizationType
			FROM keeper_meds
		) AS V
		ORDER BY StudyPatientID, RandomizationType, study_day, score DESC
;


			
/* **************************** */

SELECT 've3 build is finished' AS Progress;


