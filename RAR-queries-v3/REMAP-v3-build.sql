/*
REMAP-v3-build.sql
created by ajk77.github.io | @ajk77onX

NAVIGATION: 
	TABLE BUILD ORDER 
	REMAP.v3ModifiedENCOUNTER_PHI -> FROM CT_DATA.ENCOUNTER_PHI, REMAP.ManualChange_Encounter_FIN
	REMAP.v3CernerEnrolledPerson2 -> FROM CT_DATA.ENCOUNTER_ALL, REMAP.v3ModifiedENCOUNTER_PHI, CA_DATA.ENROLLED; (replaces REMAP.v3ViewCernerEnrolledPerson2)
	REMAP.v3Participant -> FROM REMAP.v3CernerEnrolledPerson2
	REMAP.v3IdMap -> FROM REMAP.v3CernerEnrolledPerson2, CT_DATA.ENCOUNTER_ALL, CT_DATA.CODE_VALUE  
	REMAP.v3LocOrder -> FROM CT_DATA.ENCNTR_LOC_HIST, REMAP.v3IdMap
	REMAP.v3tempLastEvent -> FROM for determing undocumented discharge >> used for updating REMAP.v3LocOrder to close off open loc entries
	REMAP.v3Lab -> FROM CT_DATA.CE_LAB, REMAP.v3IdMap, COVID_SUPPLEMENT.CV_STANDARDIZATION, CT_DATA.CODE_VALUE
	REMAP.tempv3Physio -> FROM CT_DATA.CE_PHYSIO
	REMAP.v3Physio -> FROM REMAP.tempv3Physio, REMAP.v3IdMap, COVID_SUPPLEMENT.CV_STANDARDIZATION, CT_DATA.CODE_VALUE
	REMAP.v3PhysioStr -> FROM REMAP.tempv3Physio, REMAP.v3IdMap, COVID_SUPPLEMENT.CV_STANDARDIZATION, CT_DATA.CODE_VALUE
	REMAP.v3IO -> FROM CT_DATA.CE_INTAKE_OUTPUT_RESULT, REMAP.v3IdMap, COVID_SUPPLEMENT.CV_STANDARDIZATION
	REMAP.v3Med -> FROM CT_DATA.MAR_AD, REMAP.v3IdMap, COVID_SUPPLEMENT.CV_STANDARDIZATION, CT_DATA.CODE_VALUE
	REMAP.v3OrganSupportInstance -> FROM REMAP.v3Med, REMAP.v3PhysioStr, REMAP.v3Physio, REMAP.NIVexclusion
	REMAP.v3RandomizedSevere -> FROM REMAP.v3Participant, REMAP.v3LocOrder, REMAP.v3OrganSupportInstance, COVID_SUPPLEMENT.UNIT_DESCRIPTION_ARCHIVE (updated from REMAP.HistoricRandomizationTimes)
	REMAP.v3RandomizedModerate -> FROM REMAP.v3Participant, REMAP.v3RandomizedSevere (updated from REMAP.HistoricRandomizationTimes)
	REMAP.v3StudyDay -> FROM REMAP.v3RandomizedModerate, REMAP.v3RandomizedSevere, COVID_SUPPLEMENT.STUDY_DAY, REMAP.v3LocOrder
	REMAP.v3tempDefinedLocOrder -> FROM REMAP.v3LocOrder, REMAP.v3OrganSupportInstance, COVID_SUPPLEMENT.UNIT_DESCRIPTION_ARCHIVE, CT_DATA.CODE_VALUE
	REMAP.v3UnitStay -> FROM REMAP.v3tempDefinedLocOrder
	REMAP.v3IcuStay -> FROM REMAP.v3UnitStay
	REMAP.v3RRTInstance -> FROM REMAP.v3IO, REMAP.v3Physio
	REMAP.v3SupplementalOxygenInstance -> FROM REMAP.v3PhysioStr , REMAP.v3Physio
	REMAP.v3CalculatedHourlyFiO2 -> FROM REMAP.tempPnearestF, REMAP.tempPnearestE
	REMAP.tempPnearestF -> FROM REMAP.v3Lab, REMAP.v3CalculatedHourlyFiO2  
	REMAP.tempPnearestE -> FROM REMAP.v3Lab, REMAP.v3Physio
	REMAP.v3CalculatedPFratio -> FROM REMAP.tempPnearestF, REMAP.tempPnearestE
	REMAP.v3CalculatedPEEPjoinFiO2 -> FROM REMAP.v3Physio, REMAP.v3CalculatedHourlyFiO2 
	REMAP.v3CalculatedStateHypoxiaAtEnroll -> FROM REMAP.v3CalculatedPFratio, REMAP.v3RandomizedModerate, REMAP.v3RandomizedSevere, REMAP.v3OrganSupportInstance
	REMAP.v3CalculatedSOFA -> FROM REMAP.v3Physio, REMAP.v3StudyDay, REMAP.v3Med, CT_DATA.MAR_AD MAR, CT_DATA.CODE_VALUE, REMAP.v3Physio
	REMAP.v3Hospitalization -> REMAP.v3Participant, REMAP.v3LocOrder, CT_DATA.ENCOUNTER_ALL, REMAP.v3IdMap, REMAP.ManualChange_StartOfHospitalization_utc, COVID_PHI.Locked_HospDisposition, COVID_PHI.Vital_Status_LTOC_to_COVID_Server
	REMAP.v3IcuAdmitDaysOnSupport -> FROM REMAP.v3IcuStay, REMAP.v3UnitStay, REMAP.v3OrganSupportInstance, REMAP.v3RandomizedModerate, REMAP.v3RandomizedSevere
	REMAP.v3IcuDaysOnSupport -> FROM REMAP.v3IcuAdmitDaysOnSupport

*/

/*
Dependencies
>REMAP.v3ViewCernerEnrolledPerson2 from REMAP-v3-definedViews.sql
>to_utc(), to_float(), get_prefix(), get_postfix(), get_physio_result_str() from REMAP.v3-definedFunctions.sql
*/


### correct updated FINs ###
DROP TABLE REMAP.v3ModifiedENCOUNTER_PHI;
CREATE TABLE REMAP.v3ModifiedENCOUNTER_PHI  
	SELECT EP.ENCNTR_ID, EP.BIRTH_DT_TM, EP.NAME_FIRST, EP.NAME_LAST, IFNULL(MC.FIN, EP.FIN) AS FIN, EP.MRN 
	FROM CT_DATA.ENCOUNTER_PHI EP
	LEFT JOIN REMAP.ManualChange_Encounter_FIN MC ON EP.ENCNTR_ID = MC.ENCNTR_ID
	WHERE EP.MRN IS NOT NULL
;

### create CernerEnrolledPerson table 
DROP TABLE REMAP.v3CernerEnrolledPerson2; 
CREATE TABLE REMAP.tempENCOUNTERS
	SELECT 	   
	 	EA.PERSON_ID, 
		EA.ENCNTR_ID,
	   all_screendates.screendate_utc,
	   all_screendates.STUDYPATIENTID,
	   all_screendates.REGIMEN
	FROM  
	   (
			# get person_id for each enrolled person.  				
			SELECT EF.STUDYPATIENTID, EF.screendate_utc, EF.REGIMEN, EA.PERSON_ID
			FROM (SELECT STUDYPATIENTID, screendate_utc, REGIMEN, FIN 
				FROM CA_DATA.ENROLLED WHERE ENROLLMENTRESULT = 'ENROLLED' AND STUDYPATIENTID IS NOT NULL) as EF
			JOIN REMAP.v3ModifiedENCOUNTER_PHI EP ON EF.FIN = LPAD(EP.fin, 13, '0')
			JOIN CT_DATA.ENCOUNTER_ALL EA ON EP.encntr_id = EA.encntr_id
		) AS all_screendates
		LEFT JOIN CT_DATA.ENCOUNTER_ALL as EA  ON EA.PERSON_ID = all_screendates.PERSON_ID
; 
CREATE TABLE REMAP.v3CernerEnrolledPerson2 
	SELECT
	   TE.PERSON_ID,
	   E.MRN,
	   E.ENCNTR_ID,
	   E.FIN,
	   TE.screendate_utc,
	   TE.STUDYPATIENTID,
	   TE.REGIMEN
	FROM
		REMAP.tempENCOUNTERS AS TE
	   LEFT JOIN REMAP.v3ModifiedENCOUNTER_PHI as E on E.ENCNTR_ID = TE.ENCNTR_ID
	WHERE E.FIN IS NOT NULL 
;
DROP TABLE REMAP.tempENCOUNTERS;

### pull studypateintids from view ###
DROP TABLE REMAP.v3Participant;
CREATE TABLE REMAP.v3Participant
	SELECT DISTINCT studypatientid, screendate_utc, regimen, MRN, PERSON_ID, 'Cerner' AS source_system
	FROM REMAP.v3CernerEnrolledPerson2
	ORDER BY studypatientid;
	

### link in related inpatient encntr_ids and fins. ###
DROP TABLE REMAP.v3IdMap;	
CREATE TABLE REMAP.v3IdMap
	SELECT DISTINCT V.encntr_id, fin, studypatientid, CV.display AS encounter_type
	FROM REMAP.v3CernerEnrolledPerson2 V
	JOIN CT_DATA.ENCOUNTER_ALL EA ON V.ENCNTR_ID = EA.ENCNTR_ID
	JOIN CT_DATA.CODE_VALUE CV ON EA.ENCNTR_TYPE_CD = CV.code_value
	WHERE CV.display IN ('Inpatient', 'Emergency', 'Inpt Maternity', 'Neuro Inpatient', 'Direct Obs')
;

### get locations from inpatient encounters ###
DROP TABLE REMAP.v3LocOrder;
CREATE TABLE REMAP.v3LocOrder
	SELECT IR.*, RANK() OVER ( PARTITION BY IR.STUDYPATIENTID ORDER BY  IR.beg_utc ASC) loc_order, 
		0 AS screening_location, IR.end_utc as max_end_of_prior_loc_orders
	FROM
		(SELECT DISTINCT STUDYPATIENTID, ELH.encntr_id,
			REMAP.to_utc( ELH.beg_effective_dt_tm) AS beg_utc, 
			REMAP.to_utc( ELH.end_effective_dt_tm) AS end_utc, 
			ELH.LOC_FACILITY_CD, ELH.LOC_NURSE_UNIT_CD
		FROM CT_DATA.ENCNTR_LOC_HIST ELH 
		JOIN REMAP.v3IdMap M ON ELH.ENCNTR_ID = M.ENCNTR_ID
		) AS IR
	;  
	
	## close locations without an end time ##
	CREATE TABLE REMAP.v3tempLastEvent
		WITH encntr_id_list AS
		(SELECT DISTINCT encntr_id FROM REMAP.v3IdMap
		)
		SELECT encntr_id, REMAP.to_utc(MAX(max_event_dt)) AS max_event_utc 
			FROM
				(
					SELECT 'lab', encntr_id, MAX(event_end_dt_tm) AS max_event_dt  FROM CT_DATA.CE_LAB 
					WHERE encntr_id IN (SELECT encntr_id FROM REMAP.v3IdMap) GROUP by encntr_id
				UNION
					SELECT 'physio', encntr_id, MAX(event_end_dt_tm) AS max_event_dt FROM CT_DATA.CE_PHYSIO 
					WHERE encntr_id IN (SELECT encntr_id FROM REMAP.v3IdMap) GROUP by encntr_id
				UNION
					SELECT 'med', encntr_id, MAX(event_end_dt_tm) AS max_event_dt FROM CT_DATA.CE_MED 
					WHERE encntr_id IN (SELECT encntr_id FROM REMAP.v3IdMap) GROUP by encntr_id
				) AS by_typedf
			GROUP BY encntr_id
		;
		# clost open loc entries up until most recent entry
		UPDATE REMAP.v3LocOrder O1
			JOIN REMAP.v3LocOrder O2 ON O1.studypatientid = O2.studypatientid AND O1.loc_order = O2.loc_order-1
			JOIN REMAP.v3tempLastEvent LE ON O1.encntr_id = LE.encntr_id
			SET O1.end_utc = LEAST(O2.end_utc, DATE_ADD(LE.max_event_utc, INTERVAL 24 HOUR))  # use lessser of next location and last datapoint + 24 hours
			WHERE O1.end_utc = '2100-12-31 00:00:00'
		;
		# clost open loc entries that are the most recent entry
		UPDATE REMAP.v3LocOrder O1
			JOIN REMAP.v3tempLastEvent LE ON O1.encntr_id = LE.encntr_id
			SET O1.end_utc = GREATEST(LE.max_event_utc, O1.beg_utc)  
			WHERE O1.end_utc = '2100-12-31 00:00:00' 
				AND DATE_ADD(LE.max_event_utc, INTERVAL 36 HOUR) < CURRENT_TIMESTAMP
		;
	DROP TABLE REMAP.v3tempLastEvent;

	## indicate which location entriy the pt was at time of screening ##
	UPDATE REMAP.v3LocOrder O
		JOIN REMAP.v3Participant P ON O.STUDYPATIENTID = P.STUDYPATIENTID
		SET O.screening_location = 1
		WHERE P.screendate_utc BETWEEN O.beg_utc AND O.end_utc
	;
	
	## update max_end_of_prior_loc_orders to correct values ##
	WITH max_end AS (
		SELECT O1.Studypatientid, O1.loc_order, MAX(O2.end_utc) AS max_end_of_prior_loc_orders
		FROM REMAP.v3LocOrder O1 
		LEFT JOIN REMAP.v3LocOrder O2	ON O1.studypatientid = O2.studypatientid
		WHERE O2.loc_order <= O1.loc_order
		GROUP BY O1.Studypatientid, O1.loc_order
	)
	UPDATE REMAP.v3LocOrder O1 
		LEFT JOIN max_end O2 ON O1.studypatientid = O2.studypatientid AND O1.loc_order = O2.loc_order
		SET O1.max_end_of_prior_loc_orders = O2.max_end_of_prior_loc_orders
	;

	## identify locations from enrolled hospitalization and remove others ##
	WITH hospitalStayTransition AS  # find hospitalizations that are greater than 12 hours apart
		(SELECT *
		FROM
			(SELECT O1.STUDYPATIENTID, O1.loc_order, TIMESTAMPDIFF(MINUTE, O1.max_end_of_prior_loc_orders, O2.beg_utc) AS time_delta_minutes, O1.beg_utc AS beg_for_loc_order, O2.beg_utc AS beg_for_next_loc_order
			FROM REMAP.v3LocOrder O1 
			JOIN REMAP.v3LocOrder O2 ON O1.STUDYPATIENTID = O2.STUDYPATIENTID AND O1.loc_order = O2.loc_order-1
			) AS IR
		WHERE time_delta_minutes > 60*12  # > 12 hours means end of last hospitalization 
		), screenLoc AS   # find the loc entries where patient was screened
		(SELECT STUDYPATIENTID, loc_order AS screening_loc
			FROM REMAP.v3LocOrder		
			WHERE screening_location = 1
		), startLoc AS  # find each patients first location
		(SELECT STUDYPATIENTID, loc_order
			FROM REMAP.v3LocOrder		
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
		FROM REMAP.v3LocOrder O
		JOIN locsForEnrolledHosp L ON O.STUDYPATIENTID = L.STUDYPATIENTID
		WHERE (O.loc_order < L.h_beg_loc) OR (O.loc_order > h_end_loc)
;

### Remove encounters that are not from an enrolled hospitalization ###
DELETE FROM REMAP.v3IdMap
	WHERE ENCNTR_ID NOT IN (SELECT DISTINCT ENCNTR_ID FROM REMAP.v3LocOrder)
;
 
### pull relevant labs ###
DROP TABLE REMAP.v3Lab;
CREATE TABLE REMAP.v3Lab
	SELECT L.EVENT_ID, M.STUDYPATIENTID, L.event_utc, S.sub_standard_meaning, L.prefix, L.result_float, 
		CV.display as units, L.RESULT_VAL AS documented_val, NORMAL_LOW, NORMAL_HIGH
	FROM  
		(SELECT DISTINCT EVENT_ID, ENCNTR_ID, EVENT_CD, REMAP.to_utc(EVENT_END_DT_TM) AS event_utc, 
			REMAP.get_prefix(RESULT_VAL) as prefix, REMAP.to_float(RESULT_VAL) AS result_float, result_units_cd,
			RESULT_VAL, NORMAL_LOW, NORMAL_HIGH
		FROM CT_DATA.CE_LAB
		WHERE encntr_id IN (SELECT encntr_id FROM REMAP.v3IdMap)
			AND EVENT_CD IN (SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION WHERE source_table IN ('CE_LAB')) 
		) AS L 
		JOIN REMAP.v3IdMap M ON L.ENCNTR_ID = M.ENCNTR_ID
		JOIN COVID_SUPPLEMENT.CV_STANDARDIZATION S ON L.EVENT_CD = S.source_cv
		JOIN CT_DATA.CODE_VALUE CV ON L.result_units_cd = CV.code_value
	WHERE result_float IS NOT NULL 
;  

### pull relevant physio ###
CREATE TABLE REMAP.tempv3Physio
	SELECT DISTINCT EVENT_ID, ENCNTR_ID, EVENT_CD, EVENT_END_DT_TM, RESULT_VAL, result_units_cd, NORMAL_LOW, NORMAL_HIGH 
		FROM CT_DATA.CE_PHYSIO
		WHERE encntr_id IN (SELECT encntr_id FROM REMAP.v3IdMap)
			AND EVENT_CD IN (SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION WHERE source_table IN ('CE_PHYSIO'))
	UNION  # grab CE_LAB values that should be Physio
   SELECT DISTINCT EVENT_ID, ENCNTR_ID, EVENT_CD, EVENT_END_DT_TM, RESULT_VAL, result_units_cd, NORMAL_LOW, NORMAL_HIGH
      FROM CT_DATA.CE_LAB
      WHERE encntr_id IN (SELECT encntr_id FROM REMAP.v3IdMap)
         AND EVENT_CD IN (621140161, 676173101, 3172659610, 5096472903)
	;
	## the table for numeric physio value ##
	DROP TABLE REMAP.v3Physio;
	CREATE TABLE REMAP.v3Physio
		SELECT L.EVENT_ID, M.STUDYPATIENTID, L.event_utc, S.sub_standard_meaning, L.prefix, L.result_float, 
			CV.display AS units, L.RESULT_VAL AS documented_val, NORMAL_LOW, NORMAL_HIGH
		FROM  
			(SELECT EVENT_ID, ENCNTR_ID, EVENT_CD, REMAP.to_utc(EVENT_END_DT_TM) AS event_utc, 
				REMAP.get_prefix(RESULT_VAL) as prefix, REMAP.to_float(RESULT_VAL) AS result_float, result_units_cd,
				RESULT_VAL, NORMAL_LOW, NORMAL_HIGH
			FROM REMAP.tempv3Physio
			) AS L 
			JOIN REMAP.v3IdMap M ON L.ENCNTR_ID = M.ENCNTR_ID
			JOIN COVID_SUPPLEMENT.CV_STANDARDIZATION S ON L.EVENT_CD = S.source_cv
			JOIN CT_DATA.CODE_VALUE CV ON L.result_units_cd = CV.code_value
		WHERE result_float IS NOT NULL;
	DROP TABLE REMAP.v3PhysioStr;
	CREATE TABLE REMAP.v3PhysioStr
		SELECT L.EVENT_ID, M.STUDYPATIENTID, L.event_utc, S.sub_standard_meaning, 
			REMAP.get_physio_result_str(S.sub_standard_meaning, L.result_val) AS result_str, CV.display AS units, L.result_val AS documented_text
		FROM  
			(SELECT EVENT_ID, ENCNTR_ID, EVENT_CD, REMAP.to_utc(EVENT_END_DT_TM) AS event_utc, 
				RESULT_VAL, result_units_cd
			FROM REMAP.tempv3Physio
			WHERE EVENT_CD IN (SELECT SOURCE_CV FROM COVID_SUPPLEMENT.CV_STANDARDIZATION WHERE sub_standard_meaning 
			IN ('Oxygen therapy delivery device', 'ECMO', 'Mode',  'Endotube placement', 'Tube status', 'Airway', 'Temperature (site)'))
			) AS L 
			JOIN REMAP.v3IdMap M ON L.ENCNTR_ID = M.ENCNTR_ID
			JOIN COVID_SUPPLEMENT.CV_STANDARDIZATION S ON L.EVENT_CD = S.source_cv
			JOIN CT_DATA.CODE_VALUE CV ON L.result_units_cd = CV.code_value
		WHERE RESULT_VAL IS NOT NULL;
DROP TABLE REMAP.tempv3Physio;

### pull relevant IO ###
DROP TABLE REMAP.v3IO;
CREATE TABLE REMAP.v3IO
	SELECT L.EVENT_ID, M.STUDYPATIENTID, L.event_utc, S.sub_standard_meaning, L.result_float, '' AS units
	FROM  
		(SELECT DISTINCT EVENT_ID, ENCNTR_ID, REFERENCE_EVENT_CD AS EVENT_CD, REMAP.to_utc(IO_END_DT_TM) AS event_utc, 
			IO_VOLUME AS result_float
		FROM CT_DATA.CE_INTAKE_OUTPUT_RESULT
		WHERE encntr_id IN (SELECT encntr_id FROM REMAP.v3IdMap)
			AND REFERENCE_EVENT_CD IN (SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION WHERE source_table IN ('CE_INTAKE_OUTPUT_RESULT')
			) 
		) AS L 
		JOIN REMAP.v3IdMap M ON L.ENCNTR_ID = M.ENCNTR_ID
		JOIN COVID_SUPPLEMENT.CV_STANDARDIZATION S ON L.EVENT_CD = S.source_cv
	WHERE result_float IS NOT NULL
;
 
### pull relevant meds ###
DROP TABLE REMAP.v3Med;
CREATE TABLE REMAP.v3Med
	SELECT L.EVENT_ID, M.STUDYPATIENTID, L.event_utc, S.sub_standard_meaning, L.ADMIN_DOSAGE, CVunit.display AS units, CVroute.display AS route
	FROM  
		(SELECT DISTINCT EVENT_ID, ENCNTR_ID, EVENT_CD, REMAP.to_utc(ADMIN_START_DT_TM) AS event_utc, 
			ADMIN_DOSAGE, DOSAGE_UNIT_CD, ADMIN_ROUTE_CD
		FROM CT_DATA.MAR_AD
		WHERE encntr_id IN (SELECT encntr_id FROM REMAP.v3IdMap)
			AND EVENT_CD IN (SELECT source_cv FROM COVID_SUPPLEMENT.CV_STANDARDIZATION WHERE source_table IN ('MAR_AD')
			) 
		) AS L 
		JOIN REMAP.v3IdMap M ON L.ENCNTR_ID = M.ENCNTR_ID
		JOIN COVID_SUPPLEMENT.CV_STANDARDIZATION S ON L.EVENT_CD = S.source_cv
		JOIN CT_DATA.CODE_VALUE CVunit ON L.DOSAGE_UNIT_CD = CVunit.code_value
		JOIN CT_DATA.CODE_VALUE CVroute ON L.ADMIN_ROUTE_CD = CVroute.code_value
	WHERE ADMIN_DOSAGE IS NOT NULL AND ADMIN_DOSAGE > 0
;

/* **************************** */

### Create OrganSupportTnstance ###
DROP TABLE REMAP.v3OrganSupportInstance;
	CREATE TABLE REMAP.v3OrganSupportInstance
				# VASO #
			SELECT event_id, studypatientid, event_utc, 'Vasopressor' AS support_type, NULL AS documented_source
			FROM REMAP.v3Med
			WHERE sub_standard_meaning = 'Vasopressor'
	   UNION  # HFNC #
			SELECT D.event_id, D.studypatientid, O.event_utc, 'HFNC' AS support_type, NULL AS documented_source
			FROM 
				(SELECT *
				 FROM REMAP.v3PhysioStr
				 WHERE sub_standard_meaning = 'Oxygen therapy delivery device' AND result_str = 'HFNC device'
				) AS D 
				JOIN (SELECT * 
				  FROM REMAP.v3Physio 
				  WHERE sub_standard_meaning = 'Oxygen Flow Rate' AND result_float >= 30
				) AS O ON (D.studypatientid = O.studypatientid AND TIMESTAMPDIFF(HOUR, D.event_utc, O.event_utc) BETWEEN -12 AND 12)
				JOIN (SELECT * 
				  FROM REMAP.v3Physio 
				  WHERE sub_standard_meaning = 'FiO2' AND result_float >= 40
				) AS F ON (O.studypatientid = F.studypatientid AND O.event_utc = F.event_utc)
		UNION # ECMO #
			SELECT event_id, studypatientid, event_utc, 'ECMO' AS support_type, NULL AS documented_source
			FROM REMAP.v3PhysioStr
			WHERE sub_standard_meaning = 'ECMO'
		UNION # NIV #
			SELECT event_id, NIV.studypatientid, event_utc, 'NIV' AS support_type, sub_standard_meaning AS documented_source
			FROM 
				(SELECT * 
				 FROM REMAP.v3PhysioStr		
				 WHERE (sub_standard_meaning = 'Mode' AND result_str = 'NIV mode')
					OR (sub_standard_meaning = 'Oxygen therapy delivery device' AND result_str = 'NIV device')
				 ) AS NIV 
			LEFT JOIN REMAP.NIVexclusion E ON NIV.StudyPatientID = E.StudyPatientID
			WHERE E.StudyPatientID IS NULL OR NOT event_utc BETWEEN NIV_exclusion_start_utc AND NIV_exclusion_end_utc
		UNION # IMV #
			SELECT DISTINCT event_id, studypatientid, event_utc, 'IMV' AS support_type, documented_source
			FROM
			  (SELECT *, 'Oxygen therapy delivery device' AS documented_source
				FROM REMAP.v3PhysioStr
				WHERE sub_standard_meaning = 'Oxygen therapy delivery device' AND result_str = 'IV device'
			  UNION
				SELECT *, 'Endotube placement' AS documented_source
				FROM REMAP.v3PhysioStr
				WHERE sub_standard_meaning = 'Endotube placement' AND result_str = 'IV tube present'
			  UNION
				SELECT *, 'Tube status' AS documented_source
				FROM REMAP.v3PhysioStr
				WHERE sub_standard_meaning = 'Tube status' AND result_str = 'IV status'
			  UNION			
				SELECT *, 'Airway' AS documented_source
				FROM REMAP.v3PhysioStr
				WHERE sub_standard_meaning = 'Airway' AND result_str = 'Endotracheal'
			  UNION			
				SELECT *, 'Mode' AS documented_source
				FROM REMAP.v3PhysioStr
				WHERE sub_standard_meaning = 'Mode' AND result_str = 'IV mode'
			) AS IMV
;
	
### find severe randomization times ###
DROP TABLE REMAP.v3RandomizedSevere;
CREATE TABLE REMAP.v3RandomizedSevere
	WITH screening_loc AS (
		SELECT STUDYPATIENTID, loc_order FROM REMAP.v3LocOrder WHERE screening_location = 1
	)
	SELECT P.STUDYPATIENTID, GREATEST(P.screendate_utc, earliest_ICU_organ_support_utc) AS randomized_utc
	FROM REMAP.v3Participant P
	JOIN (
		SELECT L.studypatientid, GREATEST(MIN(O.event_utc), MIN(L.beg_utc)) AS earliest_ICU_organ_support_utc
		FROM REMAP.v3LocOrder L
		JOIN screening_loc S ON L.studypatientid = S.studypatientid
		JOIN (SELECT * 
			FROM REMAP.v3OrganSupportInstance 
			) AS O ON L.studypatientid = O.studypatientid
		LEFT JOIN COVID_SUPPLEMENT.UNIT_DESCRIPTION_ARCHIVE U ON U.unit_code = L.LOC_NURSE_UNIT_CD AND L.beg_utc BETWEEN U.active_start_date AND U.active_end_date
		WHERE O.event_utc BETWEEN ADDDATE(L.beg_utc, INTERVAL -12 HOUR) AND L.end_utc
			AND unit_type IN ('ICU', 'Stepdown')
			AND L.loc_order >= S.loc_order
		GROUP BY L.studypatientid
	) AS O ON P.studypatientid = O.studypatientid
;

### find moderate randomization times ###
DROP TABLE REMAP.v3RandomizedModerate;
CREATE TABLE REMAP.v3RandomizedModerate
	SELECT P.STUDYPATIENTID, P.screendate_utc as randomized_utc
	FROM REMAP.v3Participant P
	LEFT JOIN REMAP.v3RandomizedSevere R ON P.studypatientid = R.studypatientid
	WHERE R.randomized_utc IS NULL OR R.randomized_utc > P.screendate_utc
; 

### update to include historic randomization times (through pt 211) ###
#DELETE FROM REMAP.v3RandomizedModerate WHERE STUDYPATIENTID < 0400100212; updated on 1/15/21
DELETE FROM REMAP.v3RandomizedModerate WHERE STUDYPATIENTID IN (
	SELECT DISTINCT STUDYPATIENTID FROM REMAP.HistoricRandomizationTimes);
INSERT INTO REMAP.v3RandomizedModerate 
	SELECT STUDYPATIENTID, RandomizedModerate_utc AS randomized_utc
	FROM REMAP.HistoricRandomizationTimes
	WHERE RandomizedModerate_utc IS NOT NULL;
ALTER TABLE REMAP.v3RandomizedModerate ORDER BY STUDYPATIENTID;

#DELETE FROM REMAP.v3RandomizedSevere WHERE STUDYPATIENTID < 0400100212;
DELETE FROM REMAP.v3RandomizedSevere WHERE STUDYPATIENTID IN (
	SELECT DISTINCT STUDYPATIENTID FROM REMAP.HistoricRandomizationTimes);
INSERT INTO REMAP.v3RandomizedSevere 
	SELECT STUDYPATIENTID, RandomizedSevere_utc AS randomized_utc
	FROM REMAP.HistoricRandomizationTimes
	WHERE RandomizedSevere_utc IS NOT NULL;
ALTER TABLE REMAP.v3RandomizedSevere ORDER BY STUDYPATIENTID;

### define study days within the first 30 ###
DROP TABLE REMAP.v3StudyDay;
	CREATE TABLE REMAP.v3StudyDay
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
					 FROM REMAP.v3RandomizedModerate
					 JOIN COVID_SUPPLEMENT.STUDY_DAY ON 1 = 1
				 UNION
				  SELECT studypatientID, randomized_utc, DATE(REMAP.to_local(randomized_utc)) as randomized_date_local, 
				 	'Severe' AS RandomizationType, study_day
					 FROM REMAP.v3RandomizedSevere
					 JOIN COVID_SUPPLEMENT.STUDY_DAY ON 1 = 1
				 ) AS P
				JOIN (SELECT studypatientid, if(MAX(end_utc) = '2100-12-31 00:00:00', NULL, MAX(end_utc)) AS EndOfHospitalization_utc
				 FROM REMAP.v3LocOrder GROUP BY STUDYPATIENTID) AS L ON P.studypatientid = L.studypatientid
			) AS temp_result
		WHERE
			(EndOfHospitalization_utc IS NULL 
				OR
			day_start_utc < EndOfHospitalization_utc)
				AND 
			day_end_utc < CURRENT_TIMESTAMP 
		ORDER BY STUDYPATIENTID, STUDY_DAY, RandomizationType
;

### REMAP.v3UnitStay ###
DROP TABLE REMAP.v3UnitStay;
	CREATE TABLE REMAP.v3tempDefinedLocOrder 
		SELECT L1.STUDYPATIENTID, L1.encntr_id, L1.beg_utc, L1.end_utc, L1.loc_order, 
			L1.LOC_NURSE_UNIT_CD, 
			CASE 
				WHEN U.unit_type = 'ICU' AND U.icu_type LIKE 'Pandemic%' THEN 'Pandemic ICU' 
				WHEN U.unit_type = 'ICU' THEN 'ICU'
				WHEN U.unit_type = 'Stepdown' THEN 'Stepdown'
				WHEN U.unit_type IN ('exclude', 'Procedure Unit', 'PACU') THEN 'ignore'
				ELSE U.unit_type
			END AS unit_type,
			CV.DISPLAY, L2.includes_organSupport, 
			if (U.unit_type = 'Stepdown', 1, 0) AS includes_stepdownUnit,
			if (U.unit_type = 'ICU' AND U.icu_type LIKE 'Pandemic%', 1, 0) AS includes_pandemicICU
		FROM REMAP.v3LocOrder L1
		LEFT JOIN (
			SELECT DISTINCT L.studypatientid, L.loc_order, 1 AS includes_organSupport 
			FROM REMAP.v3LocOrder L
			JOIN REMAP.v3OrganSupportInstance O ON L.studypatientid = O.studypatientid
			WHERE O.event_utc BETWEEN L.beg_utc AND L.end_utc
			) AS L2 ON L1.studypatientid = L2.studypatientid AND L1.loc_order = L2.loc_order
		LEFT JOIN COVID_SUPPLEMENT.UNIT_DESCRIPTION_ARCHIVE U ON U.unit_code = L1.LOC_NURSE_UNIT_CD AND L1.beg_utc BETWEEN U.active_start_date AND U.active_end_date
		LEFT JOIN CT_DATA.CODE_VALUE CV ON CV.CODE_VALUE = L1.LOC_NURSE_UNIT_CD 
	;
	## find contiguous unit stays ##
	CREATE TABLE REMAP.v3UnitStay
		WITH unitTransition AS  
			(SELECT D1.studypatientid, D1.beg_utc, D1.end_utc, D1.loc_order, D1.unit_type, D2.unit_type AS next_unit FROM
				REMAP.v3tempDefinedLocOrder D1 JOIN REMAP.v3tempDefinedLocOrder D2 
				ON D1.studypatientid = D2.studypatientid AND D1.loc_order = D2.loc_order-1
				WHERE D1.unit_type <> D2.unit_type OR D1.unit_type IS NULL OR D2.unit_type IS NULL 
			), unitEndpoints AS # find connections between unit transitions 
			(SELECT u1.studypatientid, MAX(u2.loc_order)+1 AS loc_order_unit_start, u1.loc_order AS loc_order_unit_end  
				FROM unitTransition u1 JOIN unitTransition u2 ON u1.studypatientid = u2.studypatientid 
				WHERE u1.loc_order > u2.loc_order
				GROUP BY u1.studypatientid, u1.loc_order
			), minLocOrder AS # towards first (b/c first unit does not get captured in unitEndpoints)
			(SELECT studypatientid, MIN(loc_order) AS min_loc_order FROM REMAP.v3tempDefinedLocOrder GROUP BY studypatientid
			), firstUnit AS # towards first
			(SELECT D.studypatientid, D.loc_order FROM REMAP.v3tempDefinedLocOrder D JOIN minLocOrder m ON D.studypatientid = m.studypatientid
				WHERE D.loc_order = m.min_loc_order
			), firstTransition AS # towards first
			(SELECT studypatientid, MIN(loc_order) AS loc_order FROM unitTransition GROUP BY studypatientid
			), unitEndpointsFirst AS # is first
			(SELECT u1.studypatientid, u1.loc_order AS loc_order_unit_start, u2.loc_order AS loc_order_unit_end 
				FROM firstUnit u1 JOIN firstTransition u2 ON u1.studypatientid = u2.studypatientid 
			), maxLocOrder AS  # towards last (b/c last unit does not get captured in unitEndpoints)
			(SELECT studypatientid, MAX(loc_order) AS max_loc_order FROM REMAP.v3tempDefinedLocOrder GROUP BY studypatientid
			), lastUnit AS # towards last
			(SELECT D.studypatientid, D.loc_order FROM REMAP.v3tempDefinedLocOrder D JOIN maxLocOrder m ON D.studypatientid = m.studypatientid
				WHERE D.loc_order = m.max_loc_order
			), lastTransition AS # towards last
			(SELECT studypatientid, MAX(loc_order)+1 AS loc_order FROM unitTransition GROUP BY studypatientid 
			), unitEndpointsLast AS # is last
			(SELECT u1.studypatientid, u2.loc_order AS loc_order_unit_start, u1.loc_order AS loc_order_unit_end 
				FROM lastUnit u1 JOIN lastTransition u2 ON u1.studypatientid = u2.studypatientid 
			), unitEndpointsAll AS 
			(SELECT *, 'first' AS q FROM unitEndpointsFirst
				UNION SELECT *, 'tran' AS q FROM unitEndpoints
				UNION SELECT *, 'last' AS q FROM unitEndpointsLast
			), organSupportLoc AS 
			(SELECT studypatientid, loc_order, includes_organSupport FROM REMAP.v3tempDefinedLocOrder WHERE includes_organSupport = 1
			)
			SELECT DISTINCT E.STUDYPATIENTID, Ds.unit_type, E.loc_order_unit_start, E.loc_order_unit_end, Ds.beg_utc, De.end_utc, 
				ifnull(I.includes_organSupport, 0) AS includes_organSupport, 0 AS includes_stepdownUnit, 
				if(Ds.unit_type IN ('ignore'), 1, 0) AS includes_ignoreUnit, 0 AS includes_pandemicICU 
			FROM unitEndpointsAll E
			JOIN REMAP.v3tempDefinedLocOrder Ds ON E.studypatientid = Ds.studypatientid AND E.loc_order_unit_start = Ds.loc_order
			JOIN REMAP.v3tempDefinedLocOrder De ON E.studypatientid = De.studypatientid AND E.loc_order_unit_end = De.loc_order
			LEFT JOIN organSupportLoc I ON E.studypatientid = I.studypatientid AND I.loc_order BETWEEN E.loc_order_unit_start AND E.loc_order_unit_end
			WHERE Ds.unit_type = De.unit_type  # where statement is need because in rare cases a pt is as two locs that start at the same time
			ORDER BY StudyPatientID, loc_order_unit_start;
	## add stepdownUnit flag ##	
	UPDATE REMAP.v3UnitStay U
	JOIN (SELECT * from REMAP.v3tempDefinedLocOrder WHERE includes_stepdownUnit = 1
	) AS L ON U.studypatientid = L.studypatientid AND L.loc_order BETWEEN U.loc_order_unit_start AND U.loc_order_unit_end
	SET U.includes_stepdownUnit = L.includes_stepdownUnit;
	## add pandemicUnit flag ##	
	UPDATE REMAP.v3UnitStay U
	JOIN (SELECT * from REMAP.v3tempDefinedLocOrder WHERE includes_pandemicICU = 1
	) AS L ON U.studypatientid = L.studypatientid AND L.loc_order BETWEEN U.loc_order_unit_start AND U.loc_order_unit_end
	SET U.includes_pandemicICU = L.includes_pandemicICU;
	## drop temp table ##
	DROP TABLE REMAP.v3tempDefinedLocOrder;

### identify ICU stays ###
DROP TABLE REMAP.v3IcuStay; 
CREATE TABLE REMAP.v3IcuStay
WITH organ_support_stay AS ( # identify ICU stays (including Stepdown/Pandemic ICU/ED stays with organ support)
	SELECT * 
	FROM REMAP.v3UnitStay
	WHERE unit_type IN ('ICU') or ( unit_type IN ('Stepdown', 'Pandemic ICU', 'ED') AND includes_organSupport = 1) 	
	), pre_start_of_a_stay AS (  # identify the start of each continuous ICU stay, i.e., no unit within 12 hr before
	SELECT Sroot.StudyPatientID, Sroot.loc_order_unit_start, Sroot.beg_utc, Sroot.unit_type
	FROM organ_support_stay Sroot
	LEFT JOIN organ_support_stay Sbefore ON Sroot.studypatientid = Sbefore.studypatientid 
		AND Sroot.beg_utc BETWEEN Sbefore.end_utc AND ADDDATE(Sbefore.end_utc, INTERVAL 12 HOUR)
		AND Sroot.loc_order_unit_start <> Sbefore.loc_order_unit_end
	WHERE Sbefore.StudyPatientID IS NULL
	), start_of_a_stay AS (  # adjust beg_utc of stay starts that are not ICU to only start at first organ support
	SELECT S.StudyPatientid, S.loc_order_unit_start, 
		IF(S.unit_type = 'ICU', S.beg_utc, GREATEST(MIN(O.event_utc), S.beg_utc)) AS beg_utc,
		IF(S.unit_type = 'ED', 1, 0) AS includes_EDUnit, beg_utc AS org_beg_utc
	FROM pre_start_of_a_stay S
	LEFT JOIN REMAP.v3OrganSupportInstance O ON S.StudyPatientid = O.StudyPatientid 
		AND O.event_utc > ADDDATE(S.beg_utc, INTERVAL -12 HOUR)
	GROUP BY  S.studyPatientid, S.loc_order_unit_start, S.beg_utc, S.unit_type
	), end_of_a_stay AS (  # identify the end of each continuous ICU stay, i.e., no unit within 12 hr after
	SELECT Sroot.StudyPatientID, Sroot.loc_order_unit_end, Sroot.end_utc
	FROM organ_support_stay Sroot
	LEFT JOIN organ_support_stay Safter ON Sroot.studypatientid = Safter.studypatientid 
		AND Sroot.end_utc BETWEEN Safter.beg_utc AND ADDDATE(Safter.beg_utc, INTERVAL 12 HOUR)
		AND Sroot.loc_order_unit_end <> Safter.loc_order_unit_start
	WHERE Safter.StudyPatientID IS NULL
	), joined AS (  # join all of the stay starts with stay ends
	SELECT S.StudyPatientID, S.loc_order_unit_start, E.loc_order_unit_end, 
		S.beg_utc, E.end_utc, S.includes_EDUnit, 
		IF(S.beg_utc <> S.org_beg_utc, 1, 0) AS beg_utc_is_organ_support_adjusted
	FROM start_of_a_stay S 
	JOIN end_of_a_stay E
	ON S.StudyPatientID = E.StudyPatientID
	WHERE S.loc_order_unit_start <= E.loc_order_unit_end
	), min_difference AS (  # determine which stay end is closest to the stay start
	SELECT StudyPatientID, loc_order_unit_start, MIN(loc_order_unit_end - loc_order_unit_start) AS min_diff
	FROM joined
	GROUP BY StudyPatientID, loc_order_unit_start
	), reduced_joined AS (  # filter to only keep the row with the closest stay end
	SELECT J.* 
	FROM joined J
	JOIN min_difference M ON J.StudyPatientID = M.studyPatientID AND J.loc_order_unit_start = M.loc_order_unit_start
	WHERE (J.loc_order_unit_end - J.loc_order_unit_start) = M.min_diff
	), core_rows AS ( # add stay_count column 
	SELECT *, (ROW_NUMBER() OVER(PARTITION BY StudyPatientID) - 1) AS stay_count
	FROM reduced_joined 
	ORDER BY StudyPatientID, loc_order_unit_start
	)
	SELECT 
		ROW_NUMBER() OVER() + 100000 AS upk, 
		C.StudyPatientID, C.stay_count, 
		C.beg_utc, C.end_utc,
		MAX(U.includes_organSupport) AS includes_organSupport,
		MAX(U.includes_stepdownUnit) AS includes_stepdownUnit,
		MAX(U.includes_ignoreUnit) AS includes_ignoreUnit,
		MAX(U.includes_pandemicICU) AS includes_pandemicICU,
		includes_EDUnit, beg_utc_is_organ_support_adjusted,
		C.loc_order_unit_start AS loc_start, C.loc_order_unit_end AS loc_end,
		GROUP_CONCAT(DISTINCT unit_type) AS unit_type_list
	FROM core_rows C
	JOIN REMAP.v3UnitStay U ON C.StudyPatientID = U.STUDYPATIENTID 
		AND (U.loc_order_unit_start BETWEEN C.loc_order_unit_start AND C.loc_order_unit_end
			OR U.loc_order_unit_end BETWEEN C.loc_order_unit_start AND C.loc_order_unit_end)
	GROUP BY C.StudyPatientID, C.stay_count, C.loc_order_unit_start, C.loc_order_unit_end,
		C.beg_utc, C.end_utc, C.includes_EDUnit, beg_utc_is_organ_support_adjusted
;



/* **************************** */

### v3RRTInstance ####
DROP TABLE REMAP.v3RRTInstance;
	CREATE TABLE REMAP.v3RRTInstance
		SELECT DISTINCT event_id, studypatientid, event_utc, 'RRT' AS support_type, documented_source
			FROM
			  (SELECT event_id, studypatientid, event_utc, '' AS prefix, 'IO' AS documented_source 
				FROM REMAP.v3IO
				WHERE sub_standard_meaning = 'RRT' AND RESULT_FLOAT > 0
			  UNION
				SELECT event_id, studypatientid, event_utc, prefix, 'Physio' AS documented_source
				FROM REMAP.v3Physio
				WHERE sub_standard_meaning = 'RRT'
			) AS RRT
;			

### v3SupplementalOxygenInstance ###
DROP TABLE REMAP.v3SupplementalOxygenInstance;
	CREATE TABLE REMAP.v3SupplementalOxygenInstance
		SELECT device.event_id, device.STUDYPATIENTID, device.event_utc, device.support_type
		FROM 
			(
			SELECT event_id, STUDYPATIENTID, event_utc, result_str AS support_type FROM REMAP.v3PhysioStr 
				WHERE sub_standard_meaning = 'Oxygen therapy delivery device' 
				AND result_str IN ('HFNC device', 'NC', 'Mask', 'Nonrebreather', 'Prebreather')	
			) AS device
			JOIN (		
				SELECT STUDYPATIENTID, event_utc, result_float AS Oxygen_Flow_Rate, units
				FROM REMAP.v3Physio WHERE sub_standard_meaning = 'Oxygen Flow Rate'
			) AS O2 ON (device.studypatientid = O2.studypatientid 
				AND TIMESTAMPDIFF(HOUR, device.event_utc, O2.event_utc) BETWEEN -12 AND 12)
		WHERE device.event_id NOT IN (SELECT event_id FROM REMAP.v3OrganSupportInstance where support_type = 'HFNC')
		UNION
		SELECT 
			D.event_id, D.studypatientid, O.event_utc, 'relaxedHF' AS support_type
		FROM 
			(SELECT *
			 FROM REMAP.v3PhysioStr
			 WHERE sub_standard_meaning = 'Oxygen therapy delivery device' AND result_str = 'HFNC device'
			) AS D 
			JOIN (SELECT * 
			  FROM REMAP.v3Physio 
			  WHERE sub_standard_meaning = 'Oxygen Flow Rate' AND result_float >= 20
			) AS O ON (D.studypatientid = O.studypatientid AND TIMESTAMPDIFF(HOUR, D.event_utc, O.event_utc) BETWEEN -12 AND 12)
			JOIN (SELECT * 
			  FROM REMAP.v3Physio 
			  WHERE sub_standard_meaning = 'FiO2' AND result_float >= 21
			) AS F ON (O.studypatientid = F.studypatientid AND O.event_utc = F.event_utc)
; 

### v3CalculatedHourlyFiO2 ###
DROP TABLE REMAP.v3CalculatedHourlyFiO2;
	CREATE TABLE REMAP.v3CalculatedHourlyFiO2 
	WITH TempOxygenationDevices AS (
		SELECT 
			device.STUDYPATIENTID, device.event_utc, device.support_type, Oxygen_Flow_Rate, units
		FROM	
			( # HFNC from organ support instance table removed from query b/c FiO2 will be documented in this case
			SELECT event_id, STUDYPATIENTID, event_utc, result_str AS support_type, '' AS documented_source 
			FROM REMAP.v3PhysioStr 
				WHERE sub_standard_meaning = 'Oxygen therapy delivery device' 
				AND result_str IN ('HFNC device', 'NC', 'Mask', 'Nonrebreather', 'Prebreather')	
			) AS device
			JOIN (		
				SELECT STUDYPATIENTID, event_utc, result_float AS Oxygen_Flow_Rate, units
				FROM REMAP.v3Physio WHERE sub_standard_meaning = 'Oxygen Flow Rate'
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
		 FROM REMAP.v3Physio 
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

# REMAP.v3calculatedPFratio
DROP TABLE REMAP.v3CalculatedPFratio;
	CREATE TABLE REMAP.tempPnearestF
		WITH PjoinF AS (
			SELECT P.STUDYPATIENTID, P.result_float as PaO2_float, P.units AS PaO2_units, P.event_utc AS PaO2_utc, 
				F.result_float AS FiO2_float,
				ifnull(F.event_utc, ADDDATE(P.event_utc, INTERVAL -1439 MINUTE)) AS FiO2_utc,  # handles cases where PaO2 is reported without an FIO2
				TIMESTAMPDIFF(MINUTE, ifnull(F.event_utc, ADDDATE(P.event_utc, INTERVAL -1439 MINUTE)), P.event_utc) AS delta_minutes
			FROM
				(SELECT * FROM REMAP.v3Lab WHERE sub_standard_meaning IN ('PaO2')) AS P
				LEFT JOIN (SELECT * FROM REMAP.v3CalculatedHourlyFiO2) AS F 
				ON P.STUDYPATIENTID = F.STUDYPATIENTID AND F.event_utc BETWEEN ADDDATE(P.event_utc, INTERVAL -24 HOUR) AND P.event_utc
		), minFs AS (
			SELECT STUDYPATIENTID, PaO2_utc, MIN(delta_minutes) AS min_delta FROM PjoinF GROUP BY STUDYPATIENTID, PaO2_utc
		)
		SELECT PF.* FROM PjoinF PF JOIN minFs F ON PF.STUDYPATIENTID = F.STUDYPATIENTID AND PF.PaO2_utc = F.PaO2_utc 
		WHERE PF.delta_minutes = F.min_delta
	;
	CREATE TABLE REMAP.tempPnearestE	
		WITH PjoinE AS (
			SELECT P.STUDYPATIENTID, P.result_float as PaO2_float, P.event_utc AS PaO2_utc, 
				E.result_float AS PEEP_float, E.event_utc AS PEEP_utc, TIMESTAMPDIFF(MINUTE, E.event_utc, P.event_utc) AS delta_minutes
			FROM
				(SELECT * FROM REMAP.v3Lab WHERE sub_standard_meaning IN ('PaO2')) AS P
				JOIN (SELECT * FROM REMAP.v3Physio WHERE sub_standard_meaning IN ('PEEP')) AS E 
				ON P.STUDYPATIENTID = E.STUDYPATIENTID AND E.event_utc BETWEEN ADDDATE(P.event_utc, INTERVAL -24 HOUR) AND P.event_utc	
		), minEs AS (
			SELECT STUDYPATIENTID, PaO2_utc, MIN(delta_minutes) AS min_delta FROM PjoinE GROUP BY STUDYPATIENTID, PaO2_utc
		)
		SELECT PE.* FROM PjoinE PE JOIN minEs E ON PE.STUDYPATIENTID = E.STUDYPATIENTID AND PE.PaO2_utc = E.PaO2_utc 
		WHERE PE.delta_minutes = E.min_delta
	;	
	CREATE TABLE REMAP.v3CalculatedPFratio
		SELECT DISTINCT PF.STUDYPATIENTID, ROUND(PF.PaO2_float/PF.FiO2_float*100,0) AS PF_ratio, 
			PF.PaO2_float, PF.PaO2_units, PF.PaO2_utc, PF.FiO2_float, PF.FiO2_utc, PE.PEEP_float, PE.PEEP_utc 
		FROM REMAP.tempPnearestF PF 
		LEFT JOIN REMAP.tempPnearestE PE ON PF.STUDYPATIENTID = PE.STUDYPATIENTID AND PF.PaO2_utc = PE.PaO2_utc
 	;
 	DROP TABLE REMAP.tempPnearestF;
	DROP TABLE REMAP.tempPnearestE
; 

/* 	##################### NEEED TO ADD TO DOC below here #########################   */

### v3CalculatedPEEPjoinFiO2 ###			
DROP TABLE REMAP.v3CalculatedPEEPjoinFiO2;
CREATE TABLE REMAP.v3CalculatedPEEPjoinFiO2
	WITH PEEPjoinFiO2 AS (
		SELECT P.STUDYPATIENTID, P.result_float as PEEP_float, P.event_utc AS PEEP_utc, 
			F.result_float AS FiO2_float, F.event_utc AS FiO2_utc, TIMESTAMPDIFF(MINUTE, F.event_utc, P.event_utc) AS delta_minutes
		FROM
			(SELECT * FROM REMAP.v3Physio WHERE sub_standard_meaning IN ('PEEP')) AS P
			JOIN REMAP.v3CalculatedHourlyFiO2 AS F 
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
DROP TABLE REMAP.v3CalculatedStateHypoxiaAtEnroll;
	CREATE TABLE REMAP.v3CalculatedStateHypoxiaAtEnroll
	WITH PFmoderate AS (
			SELECT PF.* FROM REMAP.v3CalculatedPFratio PF JOIN REMAP.v3RandomizedModerate R ON PF.STUDYPATIENTID = R.STUDYPATIENTID
			WHERE PF.PaO2_utc <= R.randomized_utc
		), minModerate AS (
			SELECT STUDYPATIENTID, MAX(PaO2_utc) AS max_PaO2_utc FROM PFmoderate GROUP BY STUDYPATIENTID 
		), PFsevere AS (
			SELECT PF.* FROM REMAP.v3CalculatedPFratio PF JOIN REMAP.v3RandomizedSevere R ON PF.STUDYPATIENTID = R.STUDYPATIENTID
			WHERE PF.PaO2_utc <= R.randomized_utc
		), minSevere AS (
			SELECT STUDYPATIENTID, MAX(PaO2_utc) AS max_PaO2_utc FROM PFsevere GROUP BY STUDYPATIENTID 
		), IMV AS ( # by definition, IMV is only possible for Severe state
			SELECT DISTINCT I.STUDYPATIENTID, 1 AS OnInvasiveVent 
			FROM 
				(SELECT STUDYPATIENTID, event_utc FROM REMAP.v3OrganSupportInstance WHERE support_type = 'IMV') AS I
				JOIN REMAP.v3RandomizedSevere R ON (I.STUDYPATIENTID = R.STUDYPATIENTID)
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
				StudyPatientID, NULL AS PF_ratio, NULL AS PaO2_float, NULL AS PaO2_units, NULL AS PaO2_utc,
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
			A.onInvasiveVent, A.PaO2_float, A.PaO2_units, A.PaO2_utc, A.PEEP_float, A.PEEP_utc, A.PF_ratio, A.FiO2_float, A.FiO2_utc 
		FROM allJoinedPlus A
;

### v3CalculatedSOFA ###
DROP TABLE REMAP.v3CalculatedSOFA;	
	CREATE TABLE REMAP.v3CalculatedSOFA
		## MAP
		SELECT CEP.StudyPatientId, SD.study_day, 1 AS score, RandomizationType
		FROM (
			SELECT StudyPatientID, event_utc, result_float
			FROM REMAP.v3Physio 
			WHERE sub_standard_meaning IN ('Blood Pressure (MAP)') AND result_float < 70
		) AS CEP	
		JOIN REMAP.v3StudyDay SD 
			ON (CEP.StudyPatientId = SD.StudyPatientId AND event_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		UNION
		## MAP-calculated 	
		SELECT StudyPatientId, study_day, 1 AS score, RandomizationType
		FROM (
			SELECT D.STUDYPATIENTID, SD.study_day, (D.result_float * 2 + S.result_float)/3 AS result_float, RandomizationType
			FROM REMAP.v3Physio AS D
			JOIN REMAP.v3Physio AS S ON (D.StudyPatientID = S.StudyPatientID AND D.event_utc = S.event_utc)			
			JOIN REMAP.v3StudyDay SD 
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
					CV.display, study_day, RandomizationType
				FROM REMAP.v3Med M
				JOIN CT_DATA.MAR_AD MAR ON M.event_id = MAR.event_id
				JOIN CT_DATA.CODE_VALUE CV ON (MAR.event_cd = CV.code_value) 
				JOIN REMAP.v3StudyDay SD ON (M.StudyPatientId = SD.StudyPatientId 
					AND event_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
				WHERE MINUTE(event_utc) = 0
			),	weight AS (
				SELECT StudyPatientID, event_utc AS weight_utc, result_float AS weight, units AS weight_units
				FROM REMAP.v3Physio
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
				REMAP.convert_vaso_to_sofa_points(TS.sub_standard_meaning, converted_dose) AS score,
				RandomizationType
			FROM keeper_meds
			JOIN COVID_SUPPLEMENT.TEXT_STANDARDIZATION TS ON (keeper_meds.display = TS.source_text)
		) AS V
		ORDER BY StudyPatientID, RandomizationType, study_day, score DESC
;

### CREATE v3Hospitalization ###
DROP TABLE REMAP.v3Hospitalization;
CREATE TABLE REMAP.v3Hospitalization
	WITH hospitalization AS (
		SELECT 
			P.StudyPatientID, 
			IFNULL(MCSH.StartOfHospitalization_utc, L.StartOfHospitalization_utc) AS StartOfHospitalization_utc,
			L2.EndOfHospitalization_utc	
		FROM REMAP.v3Participant P 
		LEFT JOIN (
			SELECT STUDYPATIENTID, MIN(beg_utc) AS StartOfHospitalization_utc
			FROM REMAP.v3LocOrder GROUP BY STUDYPATIENTID) AS L ON P.STUDYPATIENTID = L.STUDYPATIENTID
		LEFT JOIN (
			SELECT IM.StudyPatientID, MAX(REMAP.to_utc(EA.DISCH_DT_TM)) AS EndOfHospitalization_utc
			FROM CT_DATA.ENCOUNTER_ALL EA 
			JOIN REMAP.v3IdMap IM ON EA.encntr_id = IM.ENCNTR_ID
			GROUP BY IM.StudyPatientID
			) AS L2 ON P.STUDYPATIENTID = L2.STUDYPATIENTID
		LEFT JOIN REMAP.ManualChange_StartOfHospitalization_utc MCSH ON P.STUDYPATIENTID = MCSH.STUDYPATIENTID
	)
	SELECT 
		H.StudyPatientID, 
		H.StartOfHospitalization_utc,
		H.EndOfHospitalization_utc,
      IF ((EF.DeathDate IS NOT NULL 
            AND VS.DeathDate <= H.EndOfHospitalization_utc)
            OR DeceasedDisposition.StudyPatientID IS NOT NULL,
            'Yes', 'No') AS DeceasedAtDischarge
	FROM hospitalization H
	LEFT JOIN
		(SELECT StudyPatientID, DeathDate 
		FROM CA_DB.ENROLLMENT_FORM 
		WHERE StudyPatientID IS NOT NULL) AS EF ON H.STUDYPATIENTID = EF.StudyPatientID
	LEFT JOIN 
		(SELECT * FROM (SELECT CONCAT('0', StudyPatientID) AS StudyPatientID
		FROM COVID_PHI.Locked_HospDisposition 
		WHERE ROLLUP_NAME = 'Deceased') AS i_LHD) AS DeceasedDisposition ON H.StudyPatientID = DeceasedDisposition.StudyPatientID
	LEFT JOIN
		(SELECT StudyPatientID, DeathDate 
		FROM COVID_PHI.Vital_Status_LTOC_to_COVID_Server
		WHERE ninery_vital_status_desc = 'Dead') AS VS ON H.StudyPatientID = VS.StudyPatientID
	ORDER BY H.StudyPatientID
; #SELECT * FROM REMAP.v3Hospitalization;

### Create v3IcuAdmitDaysOnSupport ###
DROP TABLE REMAP.v3IcuAdmitDaysOnSupport; 
CREATE TABLE REMAP.v3IcuAdmitDaysOnSupport
	WITH ICU_support AS (
		SELECT I.StudyPatientID, I.stay_count, O.support_type, 
			MIN(O.event_utc) AS support_min_utc, MAX(O.event_utc) AS support_max_utc 
		FROM REMAP.v3OrganSupportInstance O
		JOIN REMAP.v3IcuStay I  
		ON O.StudyPatientID = I.StudyPatientID 
			AND O.event_utc BETWEEN ADDDATE(I.beg_utc, INTERVAL -24 HOUR) AND ADDDATE(I.end_utc, INTERVAL 24 HOUR)  
		GROUP BY I.StudyPatientID, I.stay_count, O.support_type
	), Joined_ICU_support AS (
		SELECT I.*, 
			Vaso.support_min_utc AS Vaso_min_utc, Vaso.support_max_utc AS Vaso_max_utc,  
			HFNC.support_min_utc AS HFNC_min_utc, HFNC.support_max_utc AS HFNC_max_utc, 
			ECMO.support_min_utc AS ECMO_min_utc, ECMO.support_max_utc AS ECMO_max_utc, 
			Niv.support_min_utc AS Niv_min_utc, Niv.support_max_utc AS Niv_max_utc, 
			IMV.support_min_utc AS IMV_min_utc, IMV.support_max_utc AS IMV_max_utc
		FROM REMAP.v3IcuStay I
		LEFT JOIN ICU_support Vaso ON I.STUDYPATIENTID = Vaso.StudyPatientID AND I.stay_count = Vaso.stay_count AND Vaso.support_type = 'Vasopressor'
		LEFT JOIN ICU_support HFNC ON I.STUDYPATIENTID = HFNC.StudyPatientID AND I.stay_count = HFNC.stay_count AND HFNC.support_type = 'HFNC' 
		LEFT JOIN ICU_support ECMO ON I.STUDYPATIENTID = ECMO.StudyPatientID AND I.stay_count = ECMO.stay_count AND ECMO.support_type = 'ECMO'
		LEFT JOIN ICU_support Niv ON I.STUDYPATIENTID = Niv.StudyPatientID AND I.stay_count = Niv.stay_count AND Niv.support_type = 'NIV'
		LEFT JOIN ICU_support IMV ON I.STUDYPATIENTID = IMV.StudyPatientID AND I.stay_count = IMV.stay_count AND IMV.support_type = 'IMV'
	), Join_ICU_support_plus AS (
		SELECT *, LEAST(REMAP.dfltH(Vaso_min_utc), REMAP.dfltH(HFNC_min_utc), REMAP.dfltH(ECMO_min_utc), REMAP.dfltH(Niv_min_utc), REMAP.dfltH(IMV_min_utc)) AS earliest_support_utc, 
			GREATEST(REMAP.dfltL(Vaso_max_utc), REMAP.dfltL(HFNC_max_utc), REMAP.dfltL(ECMO_max_utc), REMAP.dfltL(Niv_max_utc), REMAP.dfltL(IMV_max_utc)) AS lastest_suport_utc
		FROM Joined_ICU_support
	), Full_ICU_organ_support AS (			
	SELECT StudyPatientID, stay_count, 
		beg_utc, end_utc, 
		IF(earliest_support_utc = '2030-01-01 00:00:00', NULL, earliest_support_utc) AS earliest_support_utc,
		IF(lastest_suport_utc = '2010-01-01 00:00:00', NULL, lastest_suport_utc) AS latest_suport_utc,
		includes_organSupport, includes_stepdownUnit, includes_ignoreUnit, includes_EDUnit, beg_utc_is_organ_support_adjusted, 
		Vaso_min_utc, Vaso_max_utc, HFNC_min_utc, HFNC_max_utc, ECMO_min_utc, ECMO_max_utc, Niv_min_utc, Niv_max_utc, IMV_min_utc, IMV_max_utc   
	FROM Join_ICU_support_plus
	)
	SELECT 
		IF(latest_suport_utc > M.randomized_utc AND earliest_support_utc < ADDDATE(M.randomized_utc, INTERVAL 504 HOUR), 
			TIMESTAMPDIFF(MINUTE, GREATEST(earliest_support_utc, M.randomized_utc), LEAST(latest_suport_utc, ADDDATE(M.randomized_utc, INTERVAL 504 HOUR))), 
			NULL) AS minutes_on_support_M, 
		IF(latest_suport_utc > S.randomized_utc AND earliest_support_utc < ADDDATE(S.randomized_utc, INTERVAL 504 HOUR), 
			TIMESTAMPDIFF(MINUTE, GREATEST(earliest_support_utc, S.randomized_utc), LEAST(latest_suport_utc, ADDDATE(S.randomized_utc, INTERVAL 504 HOUR))), 
			NULL) AS minutes_on_support_S, 
		F.*
	FROM Full_ICU_organ_support F 
	LEFT JOIN REMAP.v3RandomizedModerate M ON F.StudyPatientID = M.StudyPatientID
	LEFT JOIN REMAP.v3RandomizedSevere S ON F.StudyPatientID = S.StudyPatientID
	ORDER BY StudyPatientID, stay_count
; # SELECT * FROM REMAP.v3IcuAdmitDaysOnSupport;

DROP TABLE REMAP.v3IcuDaysOnSupport; 
CREATE TABLE REMAP.v3IcuDaysOnSupport
	WITH hrs_M AS (
		SELECT StudyPatientID, ROUND(SUM(minutes_on_support_M)/60, 0) AS hours_on_support_M 
		FROM REMAP.v3IcuAdmitDaysOnSupport
		WHERE minutes_on_support_M IS NOT NULL 
		GROUP BY StudyPatientID
	), hrs_S AS (
		SELECT StudyPatientID, ROUND(SUM(minutes_on_support_S)/60, 0) AS hours_on_support_S  
		FROM REMAP.v3IcuAdmitDaysOnSupport
		WHERE minutes_on_support_S IS NOT NULL 
		GROUP BY StudyPatientID
	), summary AS (
		SELECT StudyPatientID, (MAX(stay_count)-MIN(stay_count)+1) AS number_of_ICU_stays,
			LEAST(REMAP.dfltH(MIN(beg_utc)), REMAP.dfltH(MIN(earliest_support_utc))) AS first_ICU_admit, # either ICU min or (ED w/ organ support -> ICU admit) min
			MAX(end_utc) AS last_ICU_discharge,
			MIN(earliest_support_utc) AS first_support_utc,
			MAX(latest_suport_utc) AS last_support_utc
		FROM REMAP.v3IcuAdmitDaysOnSupport
		GROUP BY StudyPatientID
	)
	SELECT U.*, hours_on_support_M, hours_on_support_S  
	FROM summary U 
	LEFT JOIN hrs_M M ON U.StudyPatientID = M.StudyPatientID
	LEFT JOIN hrs_S S ON U.StudyPatientID = S.StudyPatientID
	ORDER BY U.StudyPatientID
; # SELECT * FROM REMAP.v3IcuDaysOnSupport;


			
/* **************************** */

SELECT 'v3 build is finished' AS Progress;

