

/* DEPREICIATED: see REMAP.v3ViewCernerEnrolledPerson */
/*
CREATE OR REPLACE VIEW REMAP.v3ViewCernerEnrolledPerson AS
	SELECT
	   EA.PERSON_ID,
	   E.MRN,
	   E.ENCNTR_ID,
	   E.FIN,
	   latest_screendates.screendate_utc,
	   studyptids.STUDYPATIENTID,
	   latest_screendates.REGIMEN
	FROM
	   CT_DATA.ENCOUNTER_ALL as EA
	   LEFT JOIN CT_DATA.ENCOUNTER_PHI as E on E.ENCNTR_ID = EA.ENCNTR_ID
	   LEFT JOIN CA_DB.ENROLLMENT_FORM as EF on EF.FIN = E.FIN
	   LEFT JOIN (
			# get latest screendate_utc for each Person 				
			SELECT EA.PERSON_ID,	MAX(EF.SCREENDATE_UTC) AS screendate_utc, GROUP_CONCAT(EF.REGIMEN) AS REGIMEN
			FROM
			   CT_DATA.ENCOUNTER_ALL as EA
			   left join CT_DATA.ENCOUNTER_PHI as E on E.ENCNTR_ID = EA.ENCNTR_ID
			   left join CA_DB.ENROLLMENT_FORM as EF on EF.FIN = E.FIN
			GROUP BY EA.PERSON_ID
		   ) AS latest_screendates ON EA.PERSON_ID = latest_screendates.PERSON_ID	
		LEFT JOIN (
			# get studypatientid for each Person 				
			SELECT EA.PERSON_ID, EF.STUDYPATIENTID
			FROM
			   CT_DATA.ENCOUNTER_ALL as EA
			   left join CT_DATA.ENCOUNTER_PHI as E on E.ENCNTR_ID = EA.ENCNTR_ID
			   left join CA_DB.ENROLLMENT_FORM as EF on EF.FIN = E.FIN
			WHERE EF.STUDYPATIENTID IS NOT null
		   ) AS studyptids ON EA.PERSON_ID = studyptids.PERSON_ID
	WHERE
		EA.PERSON_ID IN (
			# get enrolled People 				
			SELECT EA.PERSON_ID
			FROM
			   CT_DATA.ENCOUNTER_ALL as EA
			   left join CT_DATA.ENCOUNTER_PHI as E on E.ENCNTR_ID = EA.ENCNTR_ID
				left join CA_DB.ENROLLMENT_FORM as EF on EF.FIN = E.FIN
			WHERE EF.ENROLLMENTRESULT = 'ENROLLED'
		)
			AND
	   E.FIN IS NOT NULL
;
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
			FROM (select STUDYPATIENTID, screendate_utc, REGIMEN, FIN FROM CA_DB.ENROLLMENT_FORM WHERE ENROLLMENTRESULT = 'ENROLLED') as EF
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


/* used for callculating enrollment states. */
CREATE OR REPLACE VIEW REMAP.v3ViewCernerScreenedPerson2 AS
	SELECT
	   EA.PERSON_ID,
	   E.MRN,
	   E.ENCNTR_ID,
	   E.FIN,
	   all_screendates.screendate_utc,
	   ifnull(all_screendates.STUDYPATIENTID, 9000000000 + E.MRN) AS STUDYPATIENTID,
	   ifnull(all_screendates.REGIMEN, 'NOT ENROLLED') AS REGIMEN
	FROM
	   CT_DATA.ENCOUNTER_ALL as EA
	   LEFT JOIN CT_DATA.ENCOUNTER_PHI as E on E.ENCNTR_ID = EA.ENCNTR_ID
	   LEFT JOIN CA_DB.ENROLLMENT_FORM as EF on EF.FIN = E.FIN
	   JOIN (
			# get person_id for each enrolled person.  				
			SELECT EF.STUDYPATIENTID, EF.screendate_utc, EF.REGIMEN, EA.PERSON_ID
			FROM (select STUDYPATIENTID, screendate_utc, REGIMEN, FIN FROM CA_DB.ENROLLMENT_FORM) as EF
			JOIN CT_DATA.ENCOUNTER_PHI EP ON EF.FIN = EP.fin
			JOIN CT_DATA.ENCOUNTER_ALL EA ON EP.encntr_id = EA.encntr_id
		) AS all_screendates ON EA.PERSON_ID = all_screendates.PERSON_ID	
	WHERE E.FIN IS NOT NULL 
;




# Hospital admit and discharge times
#CREATE OR REPLACE VIEW REMAP.v3EnrolledHospitalization AS
	SELECT STUDYPATIENTID, MIN(beg_utc) AS beg_utc, MAX(end_utc) AS end_utc
	FROM REMAP.v3locOrder
	GROUP BY STUDYPATIENTID
;

# Randomization time matching 
#CREATE OR REPLACE VIEW REMAP.v3RandomizationMatchingWithV2 AS
SELECT P.studypatientid, 
	P.RandomizedModerate_utc, M.randomized_utc, if(P.RandomizedModerate_utc = M.randomized_utc OR (P.RandomizedModerate_utc IS NULL AND M.randomized_utc IS NULL), TRUE, FALSE) AS mod_match,
	P.RandomizedSevere_utc, S.randomized_utc, if(P.RandomizedSevere_utc = S.randomized_utc  OR (P.RandomizedSevere_utc IS NULL AND S.randomized_utc IS NULL), TRUE, FALSE) AS sev_match
FROM COVID_PHI.v2EnrolledPerson P 
LEFT JOIN REMAP.v3RandomizedModerate M ON P.studypatientid = M.studypatientid
LEFT JOIN REMAP.v3RandomizedSevere S ON P.studypatientid = S.studypatientid
;


/* This query was run on 12/5/20 to lock in historic randomization times (up through pt 211). 
CREATE TABLE REMAP.HistoricRandomizationTimes	
	SELECT DISTINCT STUDYPATIENTID, RandomizedModerate_utc, RandomizedSevere_utc 
	FROM COVID_PHI.v2EnrolledPerson 
	WHERE STUDYPATIENTID < 0400100212
	ORDER BY STUDYPATIENTID;
*/
 