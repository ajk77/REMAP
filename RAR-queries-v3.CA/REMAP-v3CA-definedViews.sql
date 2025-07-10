/*
REMAP_CA-v3CA-definedViews.sql
created by ajk77.github.io | @ajk77onX

NAVIGATION: 
	VIEWS
	REMAP_CA.v3ViewCernerEnrolledPerson2 -> FROM CT_DATA.ENCOUNTER_ALL, REMAP_CA.v3ModifiedENCOUNTER_PHI, CA_DB.ENROLLMENT_FORM
	REMAP_CAe.ve3ViewEpicEnrolledPerson2 -> FROM COVID_PINNACLE.PAT_ENC, COVID_PINNACLE.PATIENT
	REMAP_CA.v3EnrolledHospitalization -> FROM REMAP_CA.v3locOrder
	REMAP_CA.v3RandomizationMatchingWithV2 -> FROM COVID_PHI.v2EnrolledPerson, REMAP_CA.v3RandomizedModerate,REMAP_CA.v3RandomizedSevere 
	COVID_PHI.v2ApacheeScoreS -> FROM COVID_PHI.v2ApacheeVarS, COVID_PHI.v2EnrolledPerson, COVID_PHI.GCS_scores, CA_DB.INTAKE_FORM
	COVID_PHI.v2ApacheeDebug -> FROM apachee_baseline
	REMAP_CAe.ve2ApacheeScoreS -> FROM REMAP_CAe.ve2ApacheeVarS, REMAP_CAe.ve2EnrolledPerson, COVID_PHI.GCS_scores, CA_DB.INTAKE_FORM
	REMAP_CAe.ve2ApacheeDebug -> FROM apachee_baseline

	ARCHIVED TABLE and VIEW CREATIONS
	/ REMAP_CA.HistoricRandomizationTimes /
	/ REMAP_CA.NIVexclusion /
	/ REMAP_CA.ManualChange_StartOfHospitalization_utc /
	/ REMAP_CA.ManualChange_Encounter_FIN /
*/

/* corrected to allow for a pt to be enrolled more than 1. And to trust the enrollment_form to assign the right pt id */
CREATE OR REPLACE VIEW REMAP_CA.v3ViewCernerEnrolledPerson2 AS
	SELECT DISTINCT
		EA.PERSON_ID,
	   E.MRN,
	   E.ENCNTR_ID,
	   E.FIN,
	   all_screendates.screendate_utc,
	   all_screendates.STUDYPATIENTID,
	   all_screendates.REGIMEN
	FROM
	   CA_DATA.ENCOUNTER_ALL as EA
	   LEFT JOIN REMAP_CA.v3ModifiedENCOUNTER_PHI as E on E.ENCNTR_ID = EA.ENCNTR_ID
	   LEFT JOIN CA_DB.ENROLLMENT_FORM as EF on EF.FIN = E.FIN
	   JOIN (
			# get person_id for each enrolled person.  				
			SELECT EF.STUDYPATIENTID, EF.screendate_utc, EF.REGIMEN, EA.PERSON_ID
			FROM (SELECT STUDYPATIENTID, screendate_utc, REGIMEN, FIN 
				FROM CA_DB.ENROLLMENT_FORM WHERE ENROLLMENTRESULT = 'ENROLLED' AND STUDYPATIENTID IS NOT NULL) as EF
			JOIN REMAP_CA.v3ModifiedENCOUNTER_PHI EP ON EF.FIN = EP.fin
			JOIN CA_DATA.ENCOUNTER_ALL EA ON EP.encntr_id = EA.encntr_id
		) AS all_screendates ON EA.PERSON_ID = all_screendates.PERSON_ID	
	WHERE E.FIN IS NOT NULL 
;

SELECT * from REMAP_CA.v3ViewCernerEnrolledPerson2;

/* Not updated
CREATE OR REPLACE VIEW REMAP_CAe.ve3ViewEpicEnrolledPerson2 AS
	SELECT P.PAT_ID AS PERSON_ID, P.PAT_MRN_ID AS MRN, PE.INPATIENT_DATA_ID AS ENCNTR_ID, PE.PAT_ENC_CSN_ID AS FIN, 
		EF.screendate_utc, EF.STUDYPATIENTID, EF.REGIMEN
	FROM (select STUDYPATIENTID, screendate_utc, REGIMEN, FIN FROM CA_DB.ENROLLMENT_FORM WHERE ENROLLMENTRESULT = 'ENROLLED') as EF
	JOIN COVID_PINNACLE.PAT_ENC PE ON EF.FIN = PE.PAT_ENC_CSN_ID 
	JOIN COVID_PINNACLE.PATIENT P ON PE.PAT_ID = P.PAT_ID
;
*/
/****  UNCONVERTED VIEWS *****
# Hospital admit and discharge times
CREATE OR REPLACE VIEW REMAP_CA.v3EnrolledHospitalization AS
	SELECT STUDYPATIENTID, MIN(beg_utc) AS beg_utc, MAX(end_utc) AS end_utc
	FROM REMAP_CA.v3locOrder
	GROUP BY STUDYPATIENTID
;

# Randomization time matching 
CREATE OR REPLACE VIEW REMAP_CA.v3RandomizationMatchingWithV2 AS
	SELECT P.studypatientid, 
		P.RandomizedModerate_utc, M.randomized_utc, if(P.RandomizedModerate_utc = M.randomized_utc OR (P.RandomizedModerate_utc IS NULL AND M.randomized_utc IS NULL), TRUE, FALSE) AS mod_match,
		P.RandomizedSevere_utc, S.randomized_utc, if(P.RandomizedSevere_utc = S.randomized_utc  OR (P.RandomizedSevere_utc IS NULL AND S.randomized_utc IS NULL), TRUE, FALSE) AS sev_match
	FROM COVID_PHI.v2EnrolledPerson P 
	LEFT JOIN REMAP_CA.v3RandomizedModerate M ON P.studypatientid = M.studypatientid
	LEFT JOIN REMAP_CA.v3RandomizedSevere S ON P.studypatientid = S.studypatientid
;

### Calculate ApacheeScoreS (for Severe Baseline) ###
CREATE OR REPLACE VIEW COVID_PHI.v2ApacheeScoreS AS 
	WITH apachee_baseline AS (
		SELECT *, ROW_number() over (PARTITION BY studypatientid, apachee_var, preferred_rank ORDER BY points DESC) AS rn
		FROM COVID_PHI.v2ApacheeVarS
	),
	gcs AS (
		SELECT 
			EP.StudyPatientId, EP.RandomizedSevere_utc AS RandomizationTime_utc, 
			'GCS' as sub_standard_meaning, 
			G.score, G.input_source,
			neurostatus, 
			CASE
		    	WHEN neurostatus LIKE '%15%' THEN 0
		    	WHEN neurostatus LIKE '%13%' THEN 1
	    		WHEN neurostatus LIKE '%10%' THEN 4 
		    ELSE 100
		    END AS points
		FROM
			COVID_PHI.v2EnrolledPerson EP
			LEFT JOIN (
				SELECT *, CONCAT('0', studypatientid) AS corrected_studypatientid 
				FROM COVID_PHI.GCS_scores G) AS G ON (EP.studypatientid = G.corrected_studypatientid)
			LEFT JOIN CA_DB.INTAKE_FORM I ON (EP.fin = I.fin)
		WHERE
			EP.RandomizedSevere_utc IS NOT NULL
	)
	SELECT 
		studypatientid, SUM(points) AS apachee_APS 
	FROM
		(
		SELECT 
			studypatientid, SUM(points) AS points 
		FROM
			(SELECT * FROM
				(SELECT * FROM apachee_baseline WHERE rn = 1) AS all_ranks
				JOIN 
					(SELECT studypatientid AS spi, apachee_var AS av, min(preferred_rank) AS min_rank 
					FROM apachee_baseline 
					WHERE rn = 1 
					GROUP BY studypatientid, apachee_var
				) AS min_ranks ON (all_ranks.studypatientid = min_ranks.spi AND all_ranks.apachee_var = min_ranks.av)
			WHERE
				preferred_rank = min_rank
			) AS apachee_baseline_vars_used
		GROUP BY 
			studypatientid
		UNION
		SELECT 	
			studypatientid, min(if(score IS NOT NULL, 15-score, points)) AS points
		FROM
			gcs
		GROUP BY 
			studypatientid
		) AS inner_score
	GROUP BY 
		studypatientid
; #SELECT * from COVID_PHI.v2ApacheeScoreS; 


### Create apacheeDebug view ###
CREATE OR REPLACE VIEW COVID_PHI.v2ApacheeDebug AS
	WITH apachee_baseline AS (
		SELECT *, ROW_number() over (PARTITION BY studypatientid, apachee_var, preferred_rank ORDER BY points DESC) AS rn
		FROM COVID_PHI.v2ApacheeVarS
	),
	gcs AS (
		SELECT 
			EP.StudyPatientId, EP.RandomizedSevere_utc AS RandomizationTime_utc, 
			'GCS' as sub_standard_meaning, 
			G.score, G.input_source,
			neurostatus, 
			CASE
		    	WHEN neurostatus LIKE '%15%' THEN 0
		    	WHEN neurostatus LIKE '%13%' THEN 1
	    		WHEN neurostatus LIKE '%10%' THEN 4 
		    ELSE 100
		    END AS points
		FROM
			COVID_PHI.v2EnrolledPerson EP
			LEFT JOIN (
				SELECT *, CONCAT('0', studypatientid) AS corrected_studypatientid 
				FROM COVID_PHI.GCS_scores G) AS G ON (EP.studypatientid = G.corrected_studypatientid)
			LEFT JOIN CA_DB.INTAKE_FORM I ON (EP.fin = I.fin)
		WHERE
			EP.RandomizedSevere_utc IS NOT NULL
	)
		SELECT all_ranks.* FROM
			(SELECT * FROM apachee_baseline WHERE rn = 1) AS all_ranks
			JOIN (SELECT studypatientid AS spi, apachee_var AS av, min(preferred_rank) AS min_rank FROM apachee_baseline WHERE rn = 1 GROUP BY studypatientid, apachee_var
			) AS min_ranks ON (all_ranks.studypatientid = min_ranks.spi AND all_ranks.apachee_var = min_ranks.av)
		WHERE
			preferred_rank = min_rank
		UNION
		SELECT 	
			studypatientid, 'GCS' AS apachee_var, min(score) AS result_val, min(score) AS rounded_result_val, NULL AS event_time_utc,
			min(if(score IS NOT NULL, 15-score, points)) AS points, if(min(score) IS NULL, 1, 0), 1 AS rn
		FROM
			gcs
		GROUP BY 
			studypatientid
; #SELECT * from COVID_PHI.v2ApacheeDebug; 


### Calculate ApacheeScoreS (for Severe Baseline) ###
CREATE OR REPLACE VIEW REMAP_CAe.ve2ApacheeScoreS AS 
	WITH apachee_baseline AS (
		SELECT *, ROW_number() over (PARTITION BY studypatientid, apachee_var, preferred_rank ORDER BY points DESC) AS rn
		FROM REMAP_CAe.ve2ApacheeVarS
	),
	gcs AS (
		SELECT 
			EP.StudyPatientId, EP.RandomizedSevere_utc AS RandomizationTime_utc, 
			'GCS' as sub_standard_meaning, 
			G.score, G.input_source,
			neurostatus, 
			CASE
		    	WHEN neurostatus LIKE '%15%' THEN 0
		    	WHEN neurostatus LIKE '%13%' THEN 1
	    		WHEN neurostatus LIKE '%10%' THEN 4 
		    ELSE 100
		    END AS points
		FROM
			REMAP_CAe.ve2EnrolledPerson EP
			LEFT JOIN (
				SELECT *, CONCAT('0', studypatientid) AS corrected_studypatientid 
				FROM COVID_PHI.GCS_scores G) AS G ON (EP.studypatientid = G.corrected_studypatientid)
			LEFT JOIN CA_DB.INTAKE_FORM I ON (EP.fin = I.fin)
		WHERE
			EP.RandomizedSevere_utc IS NOT NULL
	)
	SELECT 
		studypatientid, SUM(points) AS apachee_APS 
	FROM
		(
		SELECT 
			studypatientid, SUM(points) AS points 
		FROM
			(SELECT * FROM
				(SELECT * FROM apachee_baseline WHERE rn = 1) AS all_ranks
				JOIN 
					(SELECT studypatientid AS spi, apachee_var AS av, min(preferred_rank) AS min_rank 
					FROM apachee_baseline 
					WHERE rn = 1 
					GROUP BY studypatientid, apachee_var
				) AS min_ranks ON (all_ranks.studypatientid = min_ranks.spi AND all_ranks.apachee_var = min_ranks.av)
			WHERE
				preferred_rank = min_rank
			) AS apachee_baseline_vars_used
		GROUP BY 
			studypatientid
		UNION
		SELECT 	
			studypatientid, min(if(score IS NOT NULL, 15-score, points)) AS points
		FROM
			gcs
		GROUP BY 
			studypatientid
		) AS inner_score
	GROUP BY 
		studypatientid
; #SELECT * from REMAP_CAe.ve2ApacheeScoreS; 


### Create apacheeDebug view ###
CREATE OR REPLACE VIEW REMAP_CAe.ve2ApacheeDebug AS
	WITH apachee_baseline AS (
		SELECT *, ROW_number() over (PARTITION BY studypatientid, apachee_var, preferred_rank ORDER BY points DESC) AS rn
		FROM REMAP_CAe.ve2ApacheeVarS
	), gcs AS (
		SELECT 
			EP.StudyPatientId, EP.RandomizedSevere_utc AS RandomizationTime_utc, 
			'GCS' as sub_standard_meaning, 
			G.score, G.input_source,
			neurostatus, 
			CASE
		    	WHEN neurostatus LIKE '%15%' THEN 0
		    	WHEN neurostatus LIKE '%13%' THEN 1
	    		WHEN neurostatus LIKE '%10%' THEN 4 
		    ELSE 100
		    END AS points
		FROM
			REMAP_CAe.ve2EnrolledPerson EP
			LEFT JOIN (
				SELECT *, CONCAT('0', studypatientid) AS corrected_studypatientid 
				FROM COVID_PHI.GCS_scores G) AS G ON (EP.studypatientid = G.corrected_studypatientid)
			LEFT JOIN CA_DB.INTAKE_FORM I ON (EP.fin = I.fin)
		WHERE
			EP.RandomizedSevere_utc IS NOT NULL
	)
		SELECT all_ranks.* FROM
			(SELECT * FROM apachee_baseline WHERE rn = 1) AS all_ranks
			JOIN (SELECT studypatientid AS spi, apachee_var AS av, min(preferred_rank) AS min_rank FROM apachee_baseline WHERE rn = 1 GROUP BY studypatientid, apachee_var
			) AS min_ranks ON (all_ranks.studypatientid = min_ranks.spi AND all_ranks.apachee_var = min_ranks.av)
		WHERE
			preferred_rank = min_rank
		UNION
		SELECT 	
			studypatientid, 'GCS' AS apachee_var, min(score) AS result_val, min(score) AS rounded_result_val, NULL AS event_time_utc,
			min(if(score IS NOT NULL, 15-score, points)) AS points, if(min(score) IS NULL, 1, 0), 1 AS rn
		FROM
			gcs
		GROUP BY 
			studypatientid
; #SELECT * from REMAP_CAe.ve2ApacheeDebug; 

*****/


/* This query was run on 12/5/20 to lock in historic randomization times (up through pt 211). 
CREATE TABLE REMAP_CA.HistoricRandomizationTimes	
	SELECT DISTINCT STUDYPATIENTID, RandomizedModerate_utc, RandomizedSevere_utc 
	FROM COVID_PHI.v2EnrolledPerson 
	WHERE STUDYPATIENTID < 0400100212
	ORDER BY STUDYPATIENTID;
*/
 
/* This query was run on 02/04/21 to create a NIV exclusion table since the linked documention requirement was dropped. 
DROP TABLE REMAP_CA.NIVexclusion;
	CREATE TABLE REMAP_CA.NIVexclusion (
	 		upk INT AUTO_INCREMENT PRIMARY KEY,
		   StudyPatientID VARCHAR(100) NOT NULL,
	   	NIV_exclusion_start_utc DATETIME,
	      NIV_exclusion_end_utc DATETIME,
			insert_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			row_comment VARCHAR(256) DEFAULT ''
			); 
	
	# below is an example insert statement 
	INSERT INTO REMAP_CA.NIVexclusion (StudyPatientID, NIV_exclusion_start_utc, NIV_exclusion_end_utc) 
	VALUES ('0400100001', '2020-04-01 12:00:00', '2020-04-02 16:00:00');
*/

/*
DROP TABLE REMAP_CA.ManualChange_StartOfHospitalization_utc;
	CREATE TABLE REMAP_CA.ManualChange_StartOfHospitalization_utc (
	 		upk INT AUTO_INCREMENT PRIMARY KEY,
		   StudyPatientID VARCHAR(100) NOT NULL,
	   	StartOfHospitalization_utc DATETIME,
			insert_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			row_comment VARCHAR(256) DEFAULT ''		
	);

	# below is an example insert statement 
	INSERT INTO REMAP_CA.ManualChange_StartOfHospitalization_utc (StudyPatientID, StartOfHospitalization_utc, row_comment) 
	VALUES ('0400100001', '2020-04-01 12:00:00', 'AJK: replaced value b/c orginial val was 12 hours too soon.');
 */
 
 /*
CREATE TABLE REMAP_CA.ManualChange_Encounter_FIN (
	upk INT NOT NULL AUTO_INCREMENT, 
	ENCNTR_ID VARCHAR(200),
	FIN VARCHAR(200), 
	insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	row_comment VARCHAR(256), 
	PRIMARY KEY (upk)
);


*/