/*
REMAP-v3-definedViews.sql
created by King

NAVIGATION: 
	VIEWS
	REMAP.v3ViewCernerEnrolledPerson2 -> FROM CT_DATA.ENCOUNTER_ALL, CT_DATA.ENCOUNTER_PHI, CA_DB.ENROLLMENT_FORM
	REMAPe.ve3ViewEpicEnrolledPerson2 -> FROM COVID_PINNACLE.PAT_ENC, COVID_PINNACLE.PATIENT
	REMAP.v3EnrolledHospitalization -> FROM REMAP.v3locOrder
	REMAP.v3RandomizationMatchingWithV2 -> FROM COVID_PHI.v2EnrolledPerson, REMAP.v3RandomizedModerate,REMAP.v3RandomizedSevere 
	COVID_PHI.v2ApacheeScoreS -> FROM COVID_PHI.v2ApacheeVarS, COVID_PHI.v2EnrolledPerson, COVID_PHI.GCS_scores, CA_DB.INTAKE_FORM
	COVID_PHI.v2ApacheeDebug -> FROM apachee_baseline
	REMAPe.ve2ApacheeScoreS -> FROM REMAPe.ve2ApacheeVarS, REMAPe.ve2EnrolledPerson, COVID_PHI.GCS_scores, CA_DB.INTAKE_FORM
	REMAPe.ve2ApacheeDebug -> FROM apachee_baseline

	ARCHIVED TABLE CREATIONS
	/ REMAP.HistoricRandomizationTimes /
	/ REMAP.NIVexclusion /
	
*/


/* corrected to allow for a pt to be enrolled more than 1. And to trust the enrollment_form to assign the right pt id */
CREATE OR REPLACE VIEW REMAP.v3ViewCernerEnrolledPerson2 AS
	SELECT
	   EA.PERSON_ID,
	   E.MRN,
	   E.ENCNTR_ID,
	   E.FIN,
	   all_screendates.screendate_utc,
	   all_screendates.STUDYPATIENTID,
	   all_screendates.REGIMEN
	FROM
	   CT_DATA.ENCOUNTER_ALL as EA
	   LEFT JOIN CT_DATA.ENCOUNTER_PHI as E on E.ENCNTR_ID = EA.ENCNTR_ID
	   LEFT JOIN CA_DB.ENROLLMENT_FORM as EF on EF.FIN = E.FIN
	   JOIN (
			# get person_id for each enrolled person.  				
			SELECT EF.STUDYPATIENTID, EF.screendate_utc, EF.REGIMEN, EA.PERSON_ID
			FROM (SELECT STUDYPATIENTID, screendate_utc, REGIMEN, FIN 
				FROM CA_DB.ENROLLMENT_FORM WHERE ENROLLMENTRESULT = 'ENROLLED' AND STUDYPATIENTID IS NOT NULL) as EF
			JOIN CT_DATA.ENCOUNTER_PHI EP ON EF.FIN = EP.fin
			JOIN CT_DATA.ENCOUNTER_ALL EA ON EP.encntr_id = EA.encntr_id
		) AS all_screendates ON EA.PERSON_ID = all_screendates.PERSON_ID	
	WHERE E.FIN IS NOT NULL 
;


CREATE OR REPLACE VIEW REMAPe.ve3ViewEpicEnrolledPerson2 AS
	SELECT P.PAT_ID AS PERSON_ID, P.PAT_MRN_ID AS MRN, PE.INPATIENT_DATA_ID AS ENCNTR_ID, PE.PAT_ENC_CSN_ID AS FIN, 
		EF.screendate_utc, EF.STUDYPATIENTID, EF.REGIMEN
	FROM (select STUDYPATIENTID, screendate_utc, REGIMEN, FIN FROM CA_DB.ENROLLMENT_FORM WHERE ENROLLMENTRESULT = 'ENROLLED') as EF
	JOIN COVID_PINNACLE.PAT_ENC PE ON EF.FIN = PE.PAT_ENC_CSN_ID 
	JOIN COVID_PINNACLE.PATIENT P ON PE.PAT_ID = P.PAT_ID
;


# Hospital admit and discharge times
CREATE OR REPLACE VIEW REMAP.v3EnrolledHospitalization AS
	SELECT STUDYPATIENTID, MIN(beg_utc) AS beg_utc, MAX(end_utc) AS end_utc
	FROM REMAP.v3locOrder
	GROUP BY STUDYPATIENTID
;

# Randomization time matching 
CREATE OR REPLACE VIEW REMAP.v3RandomizationMatchingWithV2 AS
	SELECT P.studypatientid, 
		P.RandomizedModerate_utc, M.randomized_utc, if(P.RandomizedModerate_utc = M.randomized_utc OR (P.RandomizedModerate_utc IS NULL AND M.randomized_utc IS NULL), TRUE, FALSE) AS mod_match,
		P.RandomizedSevere_utc, S.randomized_utc, if(P.RandomizedSevere_utc = S.randomized_utc  OR (P.RandomizedSevere_utc IS NULL AND S.randomized_utc IS NULL), TRUE, FALSE) AS sev_match
	FROM COVID_PHI.v2EnrolledPerson P 
	LEFT JOIN REMAP.v3RandomizedModerate M ON P.studypatientid = M.studypatientid
	LEFT JOIN REMAP.v3RandomizedSevere S ON P.studypatientid = S.studypatientid
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
CREATE OR REPLACE VIEW REMAPe.ve2ApacheeScoreS AS 
	WITH apachee_baseline AS (
		SELECT *, ROW_number() over (PARTITION BY studypatientid, apachee_var, preferred_rank ORDER BY points DESC) AS rn
		FROM REMAPe.ve2ApacheeVarS
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
			REMAPe.ve2EnrolledPerson EP
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
; #SELECT * from REMAPe.ve2ApacheeScoreS; 


### Create apacheeDebug view ###
CREATE OR REPLACE VIEW REMAPe.ve2ApacheeDebug AS
	WITH apachee_baseline AS (
		SELECT *, ROW_number() over (PARTITION BY studypatientid, apachee_var, preferred_rank ORDER BY points DESC) AS rn
		FROM REMAPe.ve2ApacheeVarS
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
			REMAPe.ve2EnrolledPerson EP
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
; #SELECT * from REMAPe.ve2ApacheeDebug; 


/* This query was run on 12/5/20 to lock in historic randomization times (up through pt 211). 
CREATE TABLE REMAP.HistoricRandomizationTimes	
	SELECT DISTINCT STUDYPATIENTID, RandomizedModerate_utc, RandomizedSevere_utc 
	FROM COVID_PHI.v2EnrolledPerson 
	WHERE STUDYPATIENTID < 0400100212
	ORDER BY STUDYPATIENTID;
*/
 
/* This query was run on 02/04/21 to create a NIV exclusion table since the linked documention requirement was dropped. 
DROP TABLE REMAP.NIVexclusion;
	CREATE TABLE REMAP.NIVexclusion (
	 		upk INT AUTO_INCREMENT PRIMARY KEY,
		   StudyPatientID VARCHAR(100) NOT NULL,
	   	NIV_exclusion_start_utc DATETIME,
	      NIV_exclusion_end_utc DATETIME,
			insert_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			row_comment VARCHAR(256) DEFAULT ''
			); 
	
	# below is an example insert statement 
	INSERT INTO REMAP.NIVexclusion (StudyPatientID, NIV_exclusion_start_utc, NIV_exclusion_end_utc) 
	VALUES ('0400100001', '2020-04-01 12:00:00', '2020-04-02 16:00:00');
*/
 