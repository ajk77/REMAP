/*
REMAP-v3-update_v2TablesEpic.sql
created by ajk77.github.io | @ajk77onX

NAVIGATION: 
	ALL are REMAPe
	/ ve2EnrolledPerson /
	/ ve2EnrolledIcuAdmitsM /
	/ ve2EnrolledIcuAdmitsS /
	/ ve2StateHypoxiaAtEnrollM /
	/ ve2StateHypoxiaAtEnrollS /
	/ ve2HypoxiaVarM /
	/ ve2HypoxiaVarS /
	/ ve2StudyDayM /
	/ ve2StudyDayS /
	/ ve2HourlyFiO2MeasurementsM /
	/ ve2HourlyFiO2MeasurementsS /
	/ ve2ApacheeVarS / 
	
*/

	### Create ve2EnrolledPerson ###
	DROP TABLE REMAPe.ve2EnrolledPerson; 
	CREATE TABLE REMAPe.ve2EnrolledPerson
		SELECT P.PERSON_ID, P.MRN, I.ENCNTR_ID, I.FIN, P.screendate_utc, P.STUDYPATIENTID, 
			M.randomized_utc AS RandomizedModerate_utc, S.randomized_utc AS RandomizedSevere_utc, 
			H.StartOfHospitalization_utc, 
			H.EndOfHospitalization_utc,
			H.DeceasedAtDischarge, 
			P.REGIMEN, CURRENT_TIMESTAMP AS last_update 
		FROM REMAPe.ve3Participant P
		JOIN REMAPe.ve3IdMap I ON P.StudyPatientID = I.StudyPatientID
		LEFT JOIN REMAPe.ve3RandomizedModerate M ON P.StudyPatientID = M.StudyPatientID
		LEFT JOIN REMAPe.ve3RandomizedSevere S ON P.StudyPatientID = S.StudyPatientID
		LEFT JOIN REMAPe.ve3Hospitalization H	ON P.StudyPatientID = H.StudyPatientID
		ORDER BY StudyPatientID, ENCNTR_ID
	; 
	SELECT * FROM REMAPe.ve2EnrolledPerson;
	

	### Create ve2EnrolledIcuAdmitsM ###
	DROP TABLE REMAPe.ve2EnrolledIcuAdmitsM;
	CREATE TABLE REMAPe.ve2EnrolledIcuAdmitsM
		SELECT DISTINCT S.upk AS stay_id, S.StudyPatientID, P.PERSON_ID, I.encntr_id, I.fin, S.stay_count,
			S.beg_utc AS start_dt_utc, S.end_utc AS end_dt_utc, 
			TIMESTAMPDIFF(HOUR, R.randomized_utc, S.beg_utc) AS start_hr,   
			TIMESTAMPDIFF(HOUR, R.randomized_utc, S.end_utc) AS end_hr, 
			S.includes_stepdownUnit AS includes_stepdown,
			CURRENT_TIMESTAMP AS last_update 
		FROM REMAPe.ve3IcuStay S
		JOIN REMAPe.ve3RandomizedModerate R ON S.STUDYPATIENTID = R.STUDYPATIENTID
		JOIN REMAPe.ve3LocOrder L ON S.STUDYPATIENTID = L.STUDYPATIENTID AND S.loc_start = L.loc_order
		JOIN REMAPe.ve3Participant P ON S.STUDYPATIENTID = P.STUDYPATIENTID
		JOIN REMAPe.ve3IdMap I ON S.STUDYPATIENTID = I.STUDYPATIENTID AND L.encntr_id = I.ENCNTR_ID
		ORDER BY S.STUDYPATIENTID, S.beg_utc
	;


	### Create ve2EnrolledIcuAdmitsS ###
	DROP TABLE REMAPe.ve2EnrolledIcuAdmitsS;
	CREATE TABLE REMAPe.ve2EnrolledIcuAdmitsS
		SELECT DISTINCT S.upk AS stay_id, S.StudyPatientID, P.PERSON_ID, I.encntr_id, I.fin, S.stay_count,
			S.beg_utc AS start_dt_utc, S.end_utc AS end_dt_utc, 
			TIMESTAMPDIFF(HOUR, R.randomized_utc, S.beg_utc) AS start_hr,   
			TIMESTAMPDIFF(HOUR, R.randomized_utc, S.end_utc) AS end_hr, 
			S.includes_stepdownUnit AS includes_stepdown,
			CURRENT_TIMESTAMP AS last_update 
		FROM REMAPe.ve3IcuStay S
		JOIN REMAPe.ve3RandomizedSevere R ON S.STUDYPATIENTID = R.STUDYPATIENTID
		JOIN REMAPe.ve3LocOrder L ON S.STUDYPATIENTID = L.STUDYPATIENTID AND S.loc_start = L.loc_order
		JOIN REMAPe.ve3Participant P ON S.STUDYPATIENTID = P.STUDYPATIENTID
		JOIN REMAPe.ve3IdMap I ON S.STUDYPATIENTID = I.STUDYPATIENTID AND L.encntr_id = I.ENCNTR_ID
		ORDER BY S.STUDYPATIENTID, S.beg_utc
	;

	### Create ve2StateHypoxiaAtEnrollM ###
	DROP TABLE REMAPe.ve2StateHypoxiaAtEnrollM;
	CREATE TABLE REMAPe.ve2StateHypoxiaAtEnrollM
		SELECT 
			R.StudyPatientID, R.randomized_utc AS RandomizationTime_utc, ifnull(H.StateHypoxia, 1) AS hypoxia_state,
			ifnull(H.onInvasiveVent, 0) AS on_mechanical_breathing_support, if(H.PaO2_utc IS NOT NULL, 1, 0) AS ABG_avaliable,
			MAX(H.PEEP_float) AS PEEP_value, H.PEEP_utc AS PEEP_dt_utc,
			MIN(H.PF_ratio) AS PF_ratio,
			MAX(H.PaO2_float) AS PaO2_value, H.PaO2_utc AS PaO2_dt_utc,
			MAX(H.FiO2_float) AS FiO2_value, H.FiO2_utc AS FiO2_dt_utc,
			CURRENT_TIMESTAMP AS last_update 
		FROM REMAPe.ve3RandomizedModerate R
		LEFT JOIN REMAPe.ve3CalculatedStateHypoxiaAtEnroll H ON R.STUDYPATIENTID = H.STUDYPATIENTID
		WHERE RandomizationType = 'Moderate' OR RandomizationType IS NULL 
		GROUP BY R.StudyPatientID, R.randomized_utc, H.StateHypoxia, H.onInvasiveVent,
			H.PEEP_utc,H.PaO2_utc, H.FiO2_utc  # grouping is needed b/c multiple values sometimes occur at the same timestamp
		ORDER BY R.StudyPatientID
	;

	### Create ve2StateHypoxiaAtEnrollS ###
	DROP TABLE REMAPe.ve2StateHypoxiaAtEnrollS;
	CREATE TABLE REMAPe.ve2StateHypoxiaAtEnrollS
		SELECT 
			R.StudyPatientID, R.randomized_utc AS RandomizationTime_utc, ifnull(H.StateHypoxia, 1) AS hypoxia_state,
			ifnull(H.onInvasiveVent, 0) AS on_mechanical_breathing_support, if(H.PaO2_utc IS NOT NULL, 1, 0) AS ABG_avaliable,
			MAX(H.PEEP_float) AS PEEP_value, H.PEEP_utc AS PEEP_dt_utc,
			MIN(H.PF_ratio) AS PF_ratio,
			MAX(H.PaO2_float) AS PaO2_value, H.PaO2_utc AS PaO2_dt_utc,
			MAX(H.FiO2_float) AS FiO2_value, H.FiO2_utc AS FiO2_dt_utc,
			CURRENT_TIMESTAMP AS last_update 
		FROM REMAPe.ve3RandomizedSevere R
		LEFT JOIN REMAPe.ve3CalculatedStateHypoxiaAtEnroll H ON R.STUDYPATIENTID = H.STUDYPATIENTID
		WHERE RandomizationType = 'Severe' OR RandomizationType IS NULL
		GROUP BY R.StudyPatientID, R.randomized_utc, H.StateHypoxia, H.onInvasiveVent,
			H.PEEP_utc,H.PaO2_utc, H.FiO2_utc  # grouping is needed b/c multiple values sometimes occur at the same timestamp
		ORDER BY R.StudyPatientID
	;

	### Create ve2HypoxiaVarM and ve2HypoxiaVarS ###
	CREATE TABLE REMAPe.ve3tempHypoxiaVar
	SELECT 
		StudyPatientID, NULL AS PaO2_value, NULL AS PaO2_dt_utc, 
		FiO2_float AS FiO2_value, FiO2_utc AS FiO2_dt_utc,	NULL AS PF_ratio,
		PEEP_float AS PEEP_value, PEEP_utc AS PEEP_dt_utc, 
		'PEEP & FiO2 pair' AS row_type, CURRENT_TIMESTAMP AS last_update 
	FROM REMAPe.ve3CalculatedPEEPjoinFiO2
	UNION
	SELECT 
		StudyPatientID, PaO2_float AS PaO2_value, PaO2_utc AS PaO2_dt_utc,
		FiO2_float AS FiO2_value, FiO2_utc AS FiO2_dt_utc, PF_ratio,
		PEEP_float AS PEEP_value, PEEP_utc as PEEP_dt_utc,
		'P:F instance' AS row_type, CURRENT_TIMESTAMP AS last_update
	FROM REMAPe.ve3CalculatedPFratio
	;
	DROP TABLE REMAPe.ve2HypoxiaVarM;
	CREATE TABLE REMAPe.ve2HypoxiaVarM
		SELECT * FROM REMAPe.ve3tempHypoxiaVar
		WHERE studypatientid IN (SELECT studypatientid FROM REMAPe.ve3RandomizedModerate)
		ORDER BY studypatientid, FiO2_dt_utc
	;
	DROP TABLE REMAPe.ve2HypoxiaVarS;
	CREATE TABLE REMAPe.ve2HypoxiaVarS
		SELECT * FROM REMAPe.ve3tempHypoxiaVar
		WHERE studypatientid IN (SELECT studypatientid FROM REMAPe.ve3RandomizedSevere)
		ORDER BY studypatientid, FiO2_dt_utc
	;
	DROP TABLE REMAPe.ve3tempHypoxiaVar;

	### ve2StudyDayM ###
	DROP TABLE REMAPe.ve2StudyDayM;
	CREATE TABLE REMAPe.ve2StudyDayM
		SELECT DISTINCT S.StudyPatientId, R.randomized_utc AS RandomizationTime_utc, S.study_day,
			S.day_date_local, S.day_start_utc, S.day_end_utc, CURRENT_TIMESTAMP as last_update, S.RandomizationType
		FROM REMAPe.ve3StudyDay S 
		JOIN REMAPe.ve3RandomizedModerate R 
		ON S.StudyPatientId = R.StudyPatientId
		WHERE RandomizationType = 'Moderate'
		ORDER BY S.StudyPatientID, STUDY_DAY DESC
	;
	
	### ve2StudyDayS ###
	DROP TABLE REMAPe.ve2StudyDayS;
	CREATE TABLE REMAPe.ve2StudyDayS
		SELECT DISTINCT S.StudyPatientId, R.randomized_utc AS RandomizationTime_utc, S.study_day,
			S.day_date_local, S.day_start_utc, S.day_end_utc, CURRENT_TIMESTAMP as last_update, S.RandomizationType
		FROM REMAPe.ve3StudyDay S 
		JOIN REMAPe.ve3RandomizedSevere R 
		ON S.StudyPatientId = R.StudyPatientId
		WHERE RandomizationType = 'Severe'
		ORDER BY S.StudyPatientID, STUDY_DAY DESC 
	;


	### ve2HourlyFiO2MeasurementsM ###
	DROP TABLE REMAPe.ve2HourlyFiO2MeasurementsM;
	CREATE TABLE REMAPe.ve2HourlyFiO2MeasurementsM
		SELECT C.StudyPatientID, SD.study_day, NULL AS study_hour, C.event_utc AS event_time_utc,
			C.result_float AS result_val, C.fio2_source
		FROM REMAPe.ve3CalculatedHourlyFiO2 C
		JOIN REMAPe.ve2StudyDayM SD 
			ON (C.StudyPatientId = SD.StudyPatientId AND event_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		ORDER BY StudyPatientID, study_day, event_time_utc
	; 


	### ve2HourlyFiO2MeasurementsS ###
	DROP TABLE REMAPe.ve2HourlyFiO2MeasurementsS;
	CREATE TABLE REMAPe.ve2HourlyFiO2MeasurementsS
		SELECT C.StudyPatientID, SD.study_day, NULL AS study_hour, C.event_utc AS event_time_utc,
			C.result_float AS result_val, C.fio2_source
		FROM REMAPe.ve3CalculatedHourlyFiO2 C
		JOIN REMAPe.ve2StudyDayS SD ON (C.StudyPatientId = SD.StudyPatientId AND event_utc BETWEEN SD.day_start_utc AND SD.day_end_utc)
		ORDER BY StudyPatientID, study_day, event_time_utc
	; 


DROP TABLE REMAPe.ve2ApacheeVarS;
CREATE TABLE REMAPe.ve2ApacheeVarS
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
		99 AS preferred_rank
	FROM
		(SELECT 
			StudyPatientID, event_utc, 
			if(result_float > 48, ROUND(((result_float-32) * 5/9), 2), result_float) AS result_val 
		FROM REMAPe.ve3Physio 
		WHERE sub_standard_meaning IN ('Temperature (conversion)', 'Temperature (metric)', 'Temperature')
		) AS temp
	JOIN REMAPe.ve3RandomizedSevere R ON temp.StudyPatientID = R.StudyPatientID 
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
					(SELECT P.* FROM REMAPe.ve3CalculatedPFratio P JOIN REMAPe.ve3RandomizedSevere R ON P.StudyPatientID = R.StudyPatientID
					WHERE P.PaO2_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc
					) AS P
					JOIN 	
					(SELECT L.* 
					FROM REMAPe.ve3Lab L JOIN REMAPe.ve3RandomizedSevere R ON L.StudyPatientID = R.StudyPatientID
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
			FROM REMAPe.ve3Physio D
			JOIN REMAPe.ve3Physio S ON (D.StudyPatientID = S.StudyPatientID AND D.event_utc = S.event_utc)	
			JOIN REMAPe.ve3RandomizedSevere R ON D.StudyPatientID = R.StudyPatientID
			WHERE D.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc		
				AND D.sub_standard_meaning IN ('Blood pressure (arterial diastolic)') 
				AND S.sub_standard_meaning IN ('Blood pressure (arterial systolic)')
			UNION # MAP as documented #
			SELECT P.StudyPatientID, P.result_float, P.event_utc, 0 AS preferred_rank	
			FROM REMAPe.ve3Physio P
			JOIN REMAPe.ve3RandomizedSevere R ON P.StudyPatientID = R.StudyPatientID
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
			FROM REMAPe.ve3Physio 
			WHERE sub_standard_meaning IN ('Heart rate')
			) AS M
		JOIN REMAPe.ve3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
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
			FROM REMAPe.ve3Physio 
			WHERE sub_standard_meaning IN ('Respiratory rate')
			) AS M
		JOIN REMAPe.ve3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
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
			FROM REMAPe.ve3Lab 
			WHERE sub_standard_meaning IN ('pH (arterial)', 'pH (arterial temp corrected)', 'pH (arterial iStat)')
			) AS M
		JOIN REMAPe.ve3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
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
			FROM REMAPe.ve3Lab 
			WHERE sub_standard_meaning IN ('Bicarbonate', 'Bicarbonate (iStat)', 'Bicarbonate (venous calc)', 
				'Bicarbonate (venous iStat calc)', 'Bicarbonate (venous iStat)', 'Bicarbonate (venous)')
			) AS M
		JOIN REMAPe.ve3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
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
			FROM REMAPe.ve3Lab 
			WHERE sub_standard_meaning IN ('Sodium')
			UNION
			SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) as rounded_result_val, 1 AS preferred_rank
			FROM REMAPe.ve3Lab 
			WHERE sub_standard_meaning IN ('Sodium (whole blood) ','Sodium (blood gas)', 'Sodium (arterial)',
				'Sodium (iStat)', 'Sodium (venous iStat)', 'Sodium (arterial iStat)','Sodium (other)')
			) AS M
		JOIN REMAPe.ve3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
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
			FROM REMAPe.ve3Lab 
			WHERE sub_standard_meaning IN ('Potassium', 'Potassium (serum)', 'Potassium (plasma)')
			UNION
			SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) as rounded_result_val, 1 AS preferred_rank
			FROM REMAPe.ve3Lab 
			WHERE sub_standard_meaning IN ('Potassium (whole blood)', 'Potassium (iStat)', 'Potassium (blood gas)',
				'Potassium (arterial)', 'Potassium (venous iStat)', 'Potassium (arterial iStat)','Potassium (venous iStat)')
			) AS M
		JOIN REMAPe.ve3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
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
			FROM REMAPe.ve3Lab 
			WHERE sub_standard_meaning IN ('Creatinine')
			UNION
			SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) as rounded_result_val, 1 AS preferred_rank
			FROM REMAPe.ve3Lab 
			WHERE sub_standard_meaning IN ('Creatinine (iStat)', 'Creatinine (other)', 'Creatinine (whole blood)')
			) AS M
		LEFT JOIN 
			(SELECT DISTINCT studypatientid, 'yes' AS has_CRF 
			FROM CA_DB.INTAKE_FORM I 
			JOIN REMAPe.ve2EnrolledPerson EP ON (I.fin = EP.fin) 
			WHERE comorbidlist LIKE '%chronic renal disease%'
			) AS CRF ON (M.studypatientid = CRF.studypatientid)
		JOIN REMAPe.ve3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
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
			FROM REMAPe.ve3Lab 
			WHERE sub_standard_meaning IN ('Hematocrit','Hematocrit (PFA)')
			UNION
			SELECT StudyPatientID, event_utc AS event_time_utc, result_float AS RESULT_VAL, 
				ROUND(result_float+.1, 0) as rounded_result_val, 1 AS preferred_rank
			FROM REMAPe.ve3Lab 
			WHERE sub_standard_meaning IN ('Hematocrit (iStat)','Hematocrit (blood gas)', 
				'Hematocrit (arterial iStat)', 'Hematocrit (venous iStat)')
			) AS M
		JOIN REMAPe.ve3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
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
			FROM REMAPe.ve3Lab 
			WHERE sub_standard_meaning IN ('White blood count')
			) AS M
		JOIN REMAPe.ve3RandomizedSevere R ON M.StudyPatientID = R.StudyPatientID 
		WHERE M.event_time_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc	
; SELECT * FROM REMAPe.ve2ApacheeVarS;
	

	


SELECT 'updatev2tablesEpic is finished' AS Progress;
		
