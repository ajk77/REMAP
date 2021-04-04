/*
REMAP-v3-reportingFormsEpic.sql
created by AndrewJKing.com | @AndrewsJourney

NAVIGATION: 
	TABLE BUILD ORDER 
	REMAPe.ve3_RAR_condensed -> FROM REMAPe.ve3Lab, REMAPe.ve3Participant, REMAPe.ve3CalculatedStateHypoxiaAtEnroll, REMAPe.ve3RandomizedModerate, REMAPe.ve3LocOrder, REMAPe.ve3RandomizedSevere, REMAPe.ve3IcuDaysOnSupport
	REMAPe.ve3_Form2Baseline_sections5to7 -> FROM REMAPe.ve3Lab, REMAPe.ve3Physio, REMAPe.ve3RandomizedModerate, REMAPe.ve3RandomizedSevere, REMAPe.ve3CalculatedPEEPjoinFiO2, REMAPe.ve3CalculatedHourlyFiO2, REMAPe.ve3CalculatedStateHypoxiaAtEnroll, REMAPe.ve3OrganSupportInstance, COVID_PHIe.ve2ApacheeScoreS, , REMAP.v3RRTInstance, REMAP.v3CalculatedSOFA
*/


/* ve3_RAR_all */ 
DROP TABLE REMAPe.ve3_RAR_condensed;
	CREATE TABLE REMAPe.ve3_RAR_condensed
		WITH all_ddimer_24hrPreScreen AS (
			SELECT L.event_id, L.studypatientid, L.event_utc,  L.result_float, L.units 
			FROM
				(SELECT * FROM REMAPe.ve3Lab where sub_standard_meaning = 'D-dimer') as L 
				JOIN REMAPe.ve3Participant R ON L.studypatientid = R.studypatientid 
					AND L.event_utc BETWEEN ADDDATE(R.screendate_utc, INTERVAL -24 HOUR) AND R.screendate_utc
		), ddimer_closest AS (
			SELECT a.event_id, a.studypatientid, a.event_utc, a.result_float, a.units
			FROM all_ddimer_24hrPreScreen a
			INNER JOIN (
			    SELECT studypatientid, MAX(event_utc) AS event_utc
			    FROM all_ddimer_24hrPreScreen
			    GROUP BY studypatientid
			) b ON a.studypatientid = b.studypatientid AND a.event_utc = b.event_utc
		), StateHypoxia_at_Randomization AS (
			SELECT StudyPatientId, MAX(StateHypoxia) AS StateHypoxia 
			FROM REMAPe.ve3CalculatedStateHypoxiaAtEnroll 
			WHERE RandomizationType = 'Moderate' 
			GROUP BY StudyPatientId
			UNION
			SELECT StudyPatientId, MAX(StateHypoxia) AS StateHypoxia 
			FROM REMAPe.ve3CalculatedStateHypoxiaAtEnroll 
			WHERE RandomizationType = 'Severe' 
				AND StudyPatientId NOT IN (
					SELECT StudyPatientId FROM REMAP.v3RandomizedModerate
				)
			GROUP BY StudyPatientId
		), last_location AS (
			SELECT STUDYPATIENTID, REMAP.to_local(MAX(end_utc)) AS EndOfHospitalization_local
			FROM REMAPe.ve3LocOrder GROUP BY STUDYPATIENTID
		), outcomesDay21M AS (
			SELECT R.StudyPatientID, IFNULL(ROUND((504-I.hours_on_support_M)/24, 0), 22) AS ModerateOutcomeDay21
			FROM REMAPe.ve3RandomizedModerate R
			LEFT JOIN REMAPe.ve3IcuDaysOnSupport I ON R.studypatientid = I.studypatientid		
		), outcomesDay21S AS (
			SELECT R.StudyPatientID, IFNULL(ROUND((504-I.hours_on_support_S)/24, 0), 22) AS SevereOutcomeDay21 
			FROM REMAPe.ve3RandomizedSevere R
			LEFT JOIN REMAPe.ve3IcuDaysOnSupport I ON R.studypatientid = I.studypatientid		
		)
		SELECT 
			P.StudyPatientID, 
			'<e>' as CountryCode, '<e>' as SiteCode, '<e>' as PatientAge, '<e>' as SexAtBirth,
			DATE_FORMAT(IFNULL(M.Randomized_utc, '1900:01:01 00:00:00'), '%d/%m/%Y %H:%i') AS DateTimeRandModerate,
			DATE_FORMAT(IFNULL(S.Randomized_utc, '1900:01:01 00:00:00'), '%d/%m/%Y %H:%i') AS DateTimeRandSevere,
			'<e>' as EligibilityDomain__VARS__, 
			'<e>' as SeverityState__VARS__, 
			'<e>' as Eligibility__VARS__, 
			'<e>' as Assignment__VARS__,
			'<e>' as Revealed__VARS__, 
			'<e>' as StrataShock, '<e>' as InfluenzaBaseline, '<e>' as InfluenzaMicroResult,
			'<e>' as StrataInfluenza, '<e>' as StrataPandemic, '<e>' as PIconfirmed, 
			IFNULL(H.StateHypoxia, 1) AS StateHypoxiaModerate,
			D.result_float AS DDimer, D.units AS aux_DDimerUnits,
			'<e>' AS DDimerCat,
			if(M.StudyPatientID IS NOT NULL, if(CURRENT_TIMESTAMP > ADDDATE(M.Randomized_utc, INTERVAL 21 DAY), 1, 0), -1) AS Day21Moderate, 
			if(S.StudyPatientID IS NOT NULL, if(CURRENT_TIMESTAMP > ADDDATE(S.Randomized_utc, INTERVAL 21 DAY), 1, 0), -1) AS Day21Severe,	
			IFNULL(OM.ModerateOutcomeDay21, 997) AS ModerateOutcomeDay21,
			IFNULL(OS.SevereOutcomeDay21, 997) AS SevereOutcomeDay21,	
			if(M.StudyPatientID IS NOT NULL, if(CURRENT_TIMESTAMP > ADDDATE(M.Randomized_utc, INTERVAL 90 DAY), 1, 0), -1) AS Day90Moderate,
			if(S.StudyPatientID IS NOT NULL, if(CURRENT_TIMESTAMP > ADDDATE(S.Randomized_utc, INTERVAL 90 DAY), 1, 0), -1) AS Day90Severe,
			'<e>' AS OutcomeDay90Moderate, '<e>' AS OutcomeDay90Severe, 
			'<e>' AS OutcomeDateTimeModerate, '<e>' AS OutcomeDateTimeSevere, 
			if(L.EndOfHospitalization_local < CURRENT_TIMESTAMP, L.EndOfHospitalization_local, NULL) AS HospitalDCdate, 
			'<e>' AS ATTACPatientID,
			DATE_FORMAT(CURRENT_TIMESTAMP, '%d/%m/%Y %H:%i') AS INSERT_DATE
		FROM REMAPe.ve3Participant P
			LEFT JOIN REMAPe.ve3RandomizedModerate M ON P.STUDYPATIENTID = M.STUDYPATIENTID
			LEFT JOIN REMAPe.ve3RandomizedSevere S ON P.STUDYPATIENTID = S.STUDYPATIENTID
			LEFT JOIN StateHypoxia_at_Randomization H ON P.STUDYPATIENTID = H.STUDYPATIENTID
			LEFT JOIN ddimer_closest D ON P.STUDYPATIENTID = D.STUDYPATIENTID
			LEFT JOIN last_location L ON P.STUDYPATIENTID = L.STUDYPATIENTID
			LEFT JOIN outcomesDay21M OM ON P.StudyPatientID = OM.StudyPatientID
			LEFT JOIN outcomesDay21S OS ON P.StudyPatientID = OS.StudyPatientID
		ORDER BY StudyPatientId
; 
SELECT * FROM REMAPe.ve3_RAR_condensed;


/* ve3_Form2Baseline_sections5to7 */ 
DROP TABLE REMAPe.ve3_Form2Baseline_sections5to7;
CREATE TABLE REMAP.v3tempBas
		WITH all_sec6_meas AS (
			SELECT *, REMAP.to_baseline_standard(sub_standard_meaning) AS baseline_standard 
			FROM REMAPe.ve3Lab 
			where sub_standard_meaning IN (
			 	'Creatinine','Cr', 'Creatinine (iStat)', 'Creatinine (whole blood)',
			 	'Platelet count','Platelet count (DIC screen)','Platelets','Platelet count (PFA)',
			 	'Bilirubin total', 'Bilirubin total (whole blood)',
			 	'Lactate', 'Lactic Acid', 'Lactic Acid (arterial)', 'Lactic Acid (venous)', 'Lactate (no data)',  'Lactate (venous)',
				'Lactate (whole blood)','Lactate (arterial iStat)','Lactate (venous iStat)', 'Lactate (arterial iStat)',
				'Lactic Acid (iStat)', 'Lactate (iStat)','Lactate (arterial respiratory)'
			)
			UNION 
			SELECT *, REMAP.to_baseline_standard(sub_standard_meaning) AS baseline_standard 
			FROM REMAPe.ve3Physio 
			WHERE sub_standard_meaning IN (
				'Glasgow Coma Score (total)'
			) 		
		), all_sec7_meas AS (
			SELECT *, REMAP.to_baseline_standard(sub_standard_meaning) AS baseline_standard 
			FROM REMAPe.ve3Lab where sub_standard_meaning IN (
				'Ferritin',	'D-dimer', 'C-reactive protein', 'Neutrophil count', 'Lymphocyte count', 
				'Troponin T','Troponin I (iStat)','Troponin I','Troponin (unknown)','Troponin (comment)',
				'INR','INR (comment)',
				'Fibrinogen', 'Temperature',
				'Bicarbonate','Bicarbonate (iStat)', 
				'Albumin'
			)
			UNION 
			SELECT *, REMAP.to_baseline_standard(sub_standard_meaning) AS baseline_standard 
			FROM REMAPe.ve3Physio 
			WHERE sub_standard_meaning IN (
				'Blood pressure (arterial systolic)', 'Blood pressure (systolic)', 'Heart rate',
				'Respiratory rate', 'Temperature', 'Temperature (conversion)', 'Temperature (metric)') 		
		), sec6_preRand AS (
			SELECT 'Moderate' as RandomizationType, R.randomized_utc, M.*
			FROM REMAPe.ve3RandomizedModerate R
			JOIN all_sec6_meas M ON R.studypatientid = M.studypatientid 
				AND M.event_utc < ADDDATE(R.randomized_utc, INTERVAL 2 HOUR) 
			UNION
			SELECT 'Severe' as RandomizationType, R.randomized_utc, M.*
			FROM REMAPe.ve3RandomizedSevere R
			JOIN all_sec6_meas M ON R.studypatientid = M.studypatientid 
				AND M.event_utc < ADDDATE(R.randomized_utc, INTERVAL 2 HOUR) 			
		), sec7_preRand AS (
			SELECT 'Moderate' as RandomizationType, R.randomized_utc, M.*
			FROM REMAPe.ve3RandomizedModerate R
			JOIN all_sec7_meas M ON R.studypatientid = M.studypatientid 
				AND M.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -8 HOUR) AND ADDDATE(R.randomized_utc, INTERVAL 2 HOUR) 
			UNION
			SELECT 'Severe' as RandomizationType, R.randomized_utc, M.*
	 		FROM REMAPe.ve3RandomizedSevere R
			JOIN all_sec7_meas M ON R.studypatientid = M.studypatientid 
				AND M.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -8 HOUR) AND ADDDATE(R.randomized_utc, INTERVAL 2 HOUR)
		), closest_meas_pre AS (  # can contain duplucate values at same event_utc
			SELECT a.*
			FROM sec6_preRand a
			INNER JOIN (
			    SELECT studypatientid, RandomizationType, baseline_standard,
				 	MIN(ABS(TIMESTAMPDIFF(SECOND, event_utc, randomized_utc))) AS smallest_event_diff
			    FROM sec6_preRand
			    GROUP BY studypatientid, RandomizationType, baseline_standard
			) AS b ON a.studypatientid = b.studypatientid 
				AND a.RandomizationType = b.RandomizationType 
				AND a.baseline_standard = b.baseline_standard
				AND ABS(TIMESTAMPDIFF(SECOND, a.event_utc, a.randomized_utc)) = b.smallest_event_diff
			UNION 
			SELECT a.*
			FROM sec7_preRand a
			INNER JOIN (
			    SELECT studypatientid, RandomizationType, baseline_standard,
				 	MIN(ABS(TIMESTAMPDIFF(SECOND, event_utc, randomized_utc))) AS smallest_event_diff
			    FROM sec7_preRand
			    GROUP BY studypatientid, RandomizationType, baseline_standard
			) AS b ON a.studypatientid = b.studypatientid 
				AND a.RandomizationType = b.RandomizationType 
				AND a.baseline_standard = b.baseline_standard
				AND ABS(TIMESTAMPDIFF(SECOND, a.event_utc, a.randomized_utc)) = b.smallest_event_diff 
		)#, closest_meas AS (  # grab max value at given event_utc
			SELECT a.*
			FROM closest_meas_pre a
			INNER JOIN (
			    SELECT studypatientid, RandomizationType, baseline_standard,
				 	MAX(result_float) AS max_result_float
			    FROM closest_meas_pre
			    GROUP BY studypatientid, RandomizationType, baseline_standard
			) AS b ON a.studypatientid = b.studypatientid 
				AND a.RandomizationType = b.RandomizationType 
				AND a.baseline_standard = b.baseline_standard
				AND a.result_float = b.max_result_float
		
	;
	CREATE TABLE REMAP.v3tempHypoxiaVar
		WITH PEEPjoinFiO2_preRand AS (
			SELECT 'Moderate' as RandomizationType, R.randomized_utc, C.*
			FROM REMAPe.ve3RandomizedModerate R
			JOIN REMAPe.ve3CalculatedPEEPjoinFiO2 C ON R.studypatientid = C.studypatientid 
				AND C.PEEP_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc
			UNION
			SELECT 'Severe' as RandomizationType, R.randomized_utc, C.*
			FROM REMAPe.ve3RandomizedSevere R
			JOIN REMAPe.ve3CalculatedPEEPjoinFiO2 C ON R.studypatientid = C.studypatientid 
				AND C.PEEP_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc 
		), FiO2_preRand AS (
			SELECT 'Moderate' as RandomizationType, R.randomized_utc, 
				C.StudyPatientID, C.event_utc AS FiO2_utc, C.result_float AS FiO2_float 
			FROM REMAPe.ve3RandomizedModerate R
			JOIN REMAPe.ve3CalculatedHourlyFiO2 C ON R.studypatientid = C.studypatientid 
				AND C.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc
			UNION
			SELECT 'Severe' as RandomizationType, R.randomized_utc, 
				C.StudyPatientID, C.event_utc AS FiO2_utc, C.result_float AS FiO2_float 
			FROM REMAPe.ve3RandomizedSevere R
			JOIN REMAPe.ve3CalculatedHourlyFiO2 C ON R.studypatientid = C.studypatientid 
				AND C.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc 
		), closest_PEEPjoinFiO2_pre AS (
			SELECT a.*
			FROM PEEPjoinFiO2_preRand a
			INNER JOIN (
			    SELECT studypatientid, RandomizationType, 
				 	MIN(ABS(TIMESTAMPDIFF(SECOND, PEEP_utc, randomized_utc))) AS smallest_event_diff
			    FROM PEEPjoinFiO2_preRand
			    GROUP BY studypatientid, RandomizationType
			) AS b ON a.studypatientid = b.studypatientid 
				AND a.RandomizationType = b.RandomizationType 
				AND ABS(TIMESTAMPDIFF(SECOND, a.PEEP_utc, a.randomized_utc)) = b.smallest_event_diff
		), closest_FiO2_preRand AS (
			SELECT a.*
			FROM FiO2_preRand a
			INNER JOIN (
			    SELECT studypatientid, RandomizationType, 
				 	MIN(ABS(TIMESTAMPDIFF(SECOND, FiO2_utc, randomized_utc))) AS smallest_event_diff
			    FROM FiO2_preRand
			    GROUP BY studypatientid, RandomizationType
			) AS b ON a.studypatientid = b.studypatientid 
				AND a.RandomizationType = b.RandomizationType 
				AND ABS(TIMESTAMPDIFF(SECOND, a.FiO2_utc, a.randomized_utc)) = b.smallest_event_diff
		), combined_stateHypoxia AS (
			SELECT R.STUDYPATIENTID, 'Moderate' AS RandomizationType, H.onInvasiveVent,
				PaO2_float,	PEEP_float, FiO2_float, PF_ratio, StateHypoxia
			FROM REMAPe.ve3RandomizedModerate R
			LEFT JOIN REMAPe.ve3CalculatedStateHypoxiaAtEnroll H 
			ON R.STUDYPATIENTID = H.STUDYPATIENTID AND H.RandomizationType = 'Moderate'
			UNION
			SELECT R.STUDYPATIENTID, 'Severe' AS RandomizationType, H.onInvasiveVent,
				PaO2_float,	PEEP_float, FiO2_float, PF_ratio, StateHypoxia  
			FROM REMAPe.ve3RandomizedSevere R 
			LEFT JOIN REMAPe.ve3CalculatedStateHypoxiaAtEnroll H 
			ON R.STUDYPATIENTID = H.STUDYPATIENTID AND H.RandomizationType = 'Severe'
		), hypoxia_meas AS (
			SELECT 
				H.STUDYPATIENTID, H.RandomizationType,
				H.onInvasiveVent AS IV_atRand,
				H.PaO2_float,
				IFNULL(H.FiO2_float, IFNULL(E.FiO2_float, F.FiO2_float)) AS FiO2_float,
				IFNULL(H.PEEP_float, E.PEEP_float) AS PEEP_float,
				H.PF_ratio, H.StateHypoxia
			FROM combined_stateHypoxia H
			LEFT JOIN closest_PEEPjoinFiO2_pre E ON H.studypatientid = E.studypatientid AND H.RandomizationType = E.RandomizationType
			LEFT JOIN closest_FiO2_preRand F ON H.studypatientid = F.studypatientid AND H.RandomizationType = F.RandomizationType
		
		), HFNC_atRand AS (
			SELECT StudyPatientID, 'Moderate' AS RandomizationType, if(StudyPatientID IN (
				SELECT DISTINCT R.STUDYPATIENTID
				FROM REMAPe.ve3RandomizedModerate R
				LEFT JOIN REMAPe.ve3OrganSupportInstance O 
				ON R.STUDYPATIENTID = O.STUDYPATIENTID
				WHERE O.support_type = 'HFNC'
					AND O.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc 
				), 1, 0) AS HFNC_atRand
			FROM REMAPe.ve3RandomizedModerate
			UNION 
			SELECT StudyPatientID, 'Severe' AS RandomizationType, if(StudyPatientID IN (
				SELECT DISTINCT R.STUDYPATIENTID
				FROM REMAPe.ve3RandomizedSevere R
				LEFT JOIN REMAPe.ve3OrganSupportInstance O 
				ON R.STUDYPATIENTID = O.STUDYPATIENTID
				WHERE O.support_type = 'HFNC'
					AND O.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc 
				), 1, 0) AS HFNC_atRand
			FROM REMAPe.ve3RandomizedSevere
		), NIV_atRand AS (
			SELECT StudyPatientID, 'Moderate' AS RandomizationType, if(StudyPatientID IN (
				SELECT DISTINCT R.STUDYPATIENTID
				FROM REMAPe.ve3RandomizedModerate R
				LEFT JOIN REMAPe.ve3OrganSupportInstance O 
				ON R.STUDYPATIENTID = O.STUDYPATIENTID
				WHERE O.support_type = 'NIV'
					AND O.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc 
				), 1, 0) AS NIV_atRand
			FROM REMAPe.ve3RandomizedModerate
			UNION 
			SELECT StudyPatientID, 'Severe' AS RandomizationType, if(StudyPatientID IN (
				SELECT DISTINCT R.STUDYPATIENTID
				FROM REMAPe.ve3RandomizedSevere R
				LEFT JOIN REMAPe.ve3OrganSupportInstance O 
				ON R.STUDYPATIENTID = O.STUDYPATIENTID
				WHERE O.support_type = 'NIV'
					AND O.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc 
				), 1, 0) AS NIV_atRand
			FROM REMAPe.ve3RandomizedSevere
		), corrected_hypoxia_meas AS ( # because PEEP depends on organ support type
			SELECT 
				H.StudyPatientID, H.RandomizationType, H.PaO2_float, H.FiO2_float,
				if(H.IV_atRand = 1 OR N.NIV_atRand = 1, H.PEEP_float, if(C.HFNC_atRand = 1, 0, NULL)) AS PEEP_float,
				H.PF_ratio, H.StateHypoxia
			FROM hypoxia_meas H
			LEFT JOIN HFNC_atRand C ON H.studypatientid = C.studypatientid AND H.RandomizationType = C.RandomizationType
			LEFT JOIN NIV_atRand N ON H.studypatientid = N.studypatientid AND H.RandomizationType = N.RandomizationType
		)
		SELECT * FROM corrected_hypoxia_meas
	;
	CREATE TABLE REMAP.v3tempBasSupp
		SELECT R.StudyPatientID, 'Moderate' AS RandomizationType, IFNULL(Bas_CardioSOFA, 0) AS Bas_CardioSOFA, 
			IF(T.StudyPatientID IS NULL, 'No', 'Yes') AS Bas_RRT, 
			IF(O.StudyPatientID IS NULL, 'No', 'Yes') AS Bas_ExtracorporealGas, 
			IF(O.StudyPatientID IS NULL, '', 'X') AS Bas_ECMO, '' AS Bas_ECCO2R
		FROM REMAPe.ve3RandomizedModerate R
		LEFT JOIN REMAPe.ve3RRTInstance T ON R.StudyPatientID = T.StudyPatientID AND T.event_utc <= R.randomized_utc
		LEFT JOIN REMAPe.ve3OrganSupportInstance O ON R.StudyPatientID = O.StudyPatientID AND O.support_type = 'ECMO' AND O.event_utc <= R.randomized_utc
		LEFT JOIN (
			SELECT R.StudyPatientID, MAX(score) AS Bas_CardioSOFA
			FROM REMAPe.ve3RandomizedModerate R
			LEFT JOIN REMAPe.ve3CalculatedSOFA C ON R.StudyPatientID = C.StudyPatientID 
			WHERE C.STUDY_DAY < 1
			GROUP BY R.StudyPatientID	
		) AS SOFA ON R.StudyPatientID = SOFA.StudyPatientID
		UNION
		SELECT R.StudyPatientID, 'Severe' AS RandomizationType, Bas_CardioSOFA, 
			IF(T.StudyPatientID IS NULL, 'No', 'Yes') AS Bas_RRT, 
			IF(O.StudyPatientID IS NULL, 'No', 'Yes') AS Bas_ExtracorporealGas, 
			IF(O.StudyPatientID IS NULL, '', 'X') AS Bas_ECMO, '' AS Bas_ECCO2R
		FROM REMAPe.ve3RandomizedSevere R
		LEFT JOIN REMAPe.ve3RRTInstance T ON R.StudyPatientID = T.StudyPatientID AND T.event_utc <= R.randomized_utc
		LEFT JOIN REMAPe.ve3OrganSupportInstance O ON R.StudyPatientID = O.StudyPatientID AND O.support_type = 'ECMO' AND O.event_utc <= R.randomized_utc
		LEFT JOIN (
			SELECT R.StudyPatientID, MAX(score) AS Bas_CardioSOFA
			FROM REMAPe.ve3RandomizedSevere R
			LEFT JOIN REMAPe.ve3CalculatedSOFA C ON R.StudyPatientID = C.StudyPatientID 
			WHERE C.STUDY_DAY < 1
			GROUP BY R.StudyPatientID	
		) AS SOFA ON R.StudyPatientID = SOFA.StudyPatientID	
	;
CREATE TABLE REMAPe.ve3_Form2Baseline_sections5to7
	SELECT 
		R.StudyPatientID, 'Moderate' AS aux_RandomizationType, CURRENT_TIMESTAMP as aux_last_update,
		'n/a' as Bas_APACHEScore,
		H.FiO2_float AS Bas_FIO2,
		H.PaO2_float AS Bas_PaO2Entered,
		'<a>' AS Bas_PaO2Units,
		'<a>' AS Bas_PaO2_mmHg,
		H.PF_ratio AS Bas_PaO2FIO2Ratio,
		H.PEEP_float AS Bas_PEEP,
		H.StateHypoxia AS Bas_HypoxicState,
		B.Bas_CardioSOFA,
		B.Bas_RRT,
		B.Bas_ExtracorporealGas,
		B.Bas_ECMO,
		B.Bas_ECCO2R,
		'<e>' AS Bas_Etomidate,
		ifnull(M_Cr.result_float, M_Cr_aux.result_float) AS Bas_CreatinineEntered,
			if(M_Cr.result_float IS NOT NULL, M_Cr.units, M_Cr_aux.units) AS Bas_Creatinine_Units,
			'<a>' AS Bas_Creatinine_mmolL,
			if(M_Cr.result_float IS NOT NULL, M_Cr.prefix, M_Cr_aux.prefix) AS Bas_Creatinine_Accuracy,
		M_Plt.result_float AS Bas_PlateletCount, 
			M_Plt.units AS Bas_PlateletCount_Units,
			'<a>' AS Bas_PlateletCount_Cellsx10_9,
			M_Plt.prefix AS Bas_PlateletCount_Accuracy, 
		ifnull(M_TBili.result_float, M_TBili_aux.result_float) AS Bas_BilirubinEntered,
			if(M_TBili.result_float IS NOT NULL, M_TBili.units, M_TBili_aux.units) AS Bas_Bilirubin_Units,
			'<a>' AS BAS_Bilirubin_Umol,
			if(M_TBili.result_float IS NOT NULL, M_TBili.prefix, M_TBili_aux.prefix) AS Bas_Bilirubin_Accuracy,	
		ifnull(M_lactate.result_float, M_lactate_aux.result_float) AS Bas_Lactate,
			if(M_lactate.result_float IS NOT NULL, M_lactate.units, M_lactate_aux.units) AS Bas_Lactate_Units,
			'<a>' AS BAS_Lactate_mmolL,
			if(M_lactate.result_float IS NOT NULL, M_lactate.prefix, M_lactate_aux.prefix) AS Bas_Lactate_Accuracy,		
		IFNULL(G.score, M_GCS.result_float) AS Bas_GlasgowComa, 
		M_ferritin.result_float AS Bas_Ferritin, 
			M_ferritin.units AS Bas_Ferritin_Accuracy, 
		M_Ddimer.result_float AS Bas_D_Dimer, 
			M_Ddimer.units AS Bas_D_Dimer_Units,
			'<a>' AS Bas_D_Dimer_ugL,
			M_Ddimer.prefix AS Bas_D_Dimer_Accuracy,
		M_CRP.result_float AS Bas_C_ReactiveProtein,
		 	M_CRP.units AS Bas_C_ReactiveProtein_Units,
		 	'<a>' AS Bas_C_ReactiveProtein_ugL,
		 	M_CRP.prefix AS Bas_C_ReactiveProtein_Accuracy,
		M_ANC.result_float AS Bas_NeutrophilCount, 
			M_ANC.units AS Bas_NeutrophilCount_Units,
			'<a>' AS Bas_NeutrophilCount_Cellsx10_9,
			M_ANC.prefix AS Bas_NeutrophilCount_Accuracy,
		M_lymphs.result_float AS Bas_LymphocyteCount, 
			M_lymphs.units AS Bas_LymphocyteCount_Units,
			'<a>' AS Bas_LymphocyteCount_Cellsx10_9,
			M_lymphs.prefix AS Bas_LymphocyteCount_Accuracy,
		'<a>' as Bas_TroponinTest,
			M_troponin.result_float AS Bas_TroponinResult, 
			M_troponin.units AS Bas_TroponinResult_Units,
			'<a>' AS Bas_TroponinResult_ngL,
			M_troponin.prefix AS Bas_TroponinResult_Accuracy,
			M_troponin.NORMAL_HIGH AS Bas_TroponinUpperLimit,
			M_troponin.units AS Bas_TroponinUpperLimit_Units,
			'<a>' AS Bas_TroponinUpperLimit_ngl,
		M_INR.result_float AS Bas_INR_or_PR,
			M_INR.prefix AS Bas_INR_or_PR_Accuracy,
		M_fibrinogen.result_float AS Bas_Fibrinogen, 
			M_fibrinogen.units AS Bas_Fibrinogen_Units,
			'<a>' AS `Bas_Fibrinogen_g/L`,
			M_fibrinogen.prefix AS Bas_Fibrinogen_Accuracy,
		M_temp.result_float AS Bas_Temperature,
			M_temp.units AS Bas_Temperature_Units,
			'<a>' AS Bas_Temperature_C,
		M_HR.result_float AS Bas_HeartRate,
		ifnull(M_sys.result_float, M_sys_aux.result_float) AS Bas_SystolicBloodPressure,
		M_RR.result_float AS Bas_RespiratoryRate,
		M_HCO3.result_float AS Bas_Bicarbonate,
			M_HCO3.units AS Bas_Bicarbonate_Units,
			'<a>' AS Bas_Bicarbonate_mEqL,
		M_albumin.result_float AS Bas_Albumin
	FROM REMAPe.ve3RandomizedModerate R
	LEFT JOIN REMAP.v3tempHypoxiaVar H ON R.studypatientid = H.studypatientid AND H.randomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBasSupp B ON R.studypatientid = B.studypatientid AND B.randomizationType = 'Moderate'	
	LEFT JOIN COVID_PHI.GCS_scores G ON R.studypatientid = G.studypatientid
	LEFT JOIN REMAP.v3tempBas M_Cr ON R.studypatientid = M_Cr.studypatientid AND M_Cr.baseline_standard = 'Cr' AND M_Cr.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_Cr_aux ON R.studypatientid = M_Cr_aux.studypatientid AND M_Cr_aux.baseline_standard = 'Cr_aux' AND M_Cr_aux.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_Plt ON R.studypatientid = M_Plt.studypatientid AND M_Plt.baseline_standard = 'Plt' AND M_Plt.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_TBili ON R.studypatientid = M_TBili.studypatientid AND M_TBili.baseline_standard = 'TBili' AND M_TBili.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_TBili_aux ON R.studypatientid = M_TBili_aux.studypatientid AND M_TBili_aux.baseline_standard = 'TBili_aux' AND M_TBili_aux.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_lactate ON R.studypatientid = M_lactate.studypatientid AND M_lactate.baseline_standard = 'lactate' AND M_lactate.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_lactate_aux ON R.studypatientid = M_lactate_aux.studypatientid AND M_lactate_aux.baseline_standard = 'lactate_aux' AND M_lactate_aux.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_GCS ON R.studypatientid = M_GCS.studypatientid AND M_GCS.baseline_standard = 'GCS' AND M_GCS.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_ferritin ON R.studypatientid = M_ferritin.studypatientid AND M_ferritin.baseline_standard = 'Ferritin' AND M_ferritin.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_Ddimer ON R.studypatientid = M_Ddimer.studypatientid AND M_Ddimer.baseline_standard = 'D-dimer' AND M_Ddimer.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_CRP ON R.studypatientid = M_CRP.studypatientid AND M_CRP.baseline_standard = 'C-reactive protein' AND M_CRP.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_ANC ON R.studypatientid = M_ANC.studypatientid AND M_ANC.baseline_standard = 'Neutrophil count' AND M_ANC.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_lymphs ON R.studypatientid = M_lymphs.studypatientid AND M_lymphs.baseline_standard = 'Lymphocyte count' AND M_lymphs.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_troponin ON R.studypatientid = M_troponin.studypatientid AND M_troponin.baseline_standard = 'Troponin' AND M_troponin.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_INR ON R.studypatientid = M_INR.studypatientid AND M_INR.baseline_standard = 'INR' AND M_INR.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_fibrinogen ON R.studypatientid = M_fibrinogen.studypatientid AND M_fibrinogen.baseline_standard = 'Fibrinogen' AND M_fibrinogen.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_temp ON R.studypatientid = M_temp.studypatientid AND M_temp.baseline_standard = 'Temp' AND M_temp.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_HR ON R.studypatientid = M_HR.studypatientid AND M_HR.baseline_standard = 'Heart rate' AND M_HR.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_sys ON R.studypatientid = M_sys.studypatientid AND M_sys.baseline_standard = 'BP_sys' AND M_sys.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_sys_aux ON R.studypatientid = M_sys_aux.studypatientid AND M_sys_aux.baseline_standard = 'BP_sys_aux' AND M_sys_aux.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_RR ON R.studypatientid = M_RR.studypatientid AND M_RR.baseline_standard = 'Respiratory rate' AND M_RR.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_HCO3 ON R.studypatientid = M_HCO3.studypatientid AND M_HCO3.baseline_standard = 'HCO3' AND M_HCO3.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_albumin ON R.studypatientid = M_albumin.studypatientid AND M_albumin.baseline_standard = 'Albumin' AND M_albumin.RandomizationType = 'Moderate'
UNION  
	SELECT 
		R.StudyPatientID, 'Severe' AS aux_RandomizationType, CURRENT_TIMESTAMP as aux_last_update,
		A.apachee_APS as Bas_APACHEScore,
		H.FiO2_float AS Bas_FIO2,
		H.PaO2_float AS Bas_PaO2Entered,
		'<a>' AS Bas_PaO2Units,
		'<a>' AS Bas_PaO2_mmHg,
		H.PF_ratio AS Bas_PaO2FIO2Ratio,
		H.PEEP_float AS Bas_PEEP,
		H.StateHypoxia AS Bas_HypoxicState,
		B.Bas_CardioSOFA,
		B.Bas_RRT,
		B.Bas_ExtracorporealGas,
		B.Bas_ECMO,
		B.Bas_ECCO2R,
		'<e>' AS Bas_Etomidate,
		ifnull(M_Cr.result_float, M_Cr_aux.result_float) AS Bas_CreatinineEntered,
			if(M_Cr.result_float IS NOT NULL, M_Cr.units, M_Cr_aux.units) AS Bas_Creatinine_Units,
			'<a>' AS Bas_Creatinine_mmolL,
			if(M_Cr.result_float IS NOT NULL, M_Cr.prefix, M_Cr_aux.prefix) AS Bas_Creatinine_Accuracy,
		M_Plt.result_float AS Bas_PlateletCount, 
			M_Plt.units AS Bas_PlateletCount_Units,
			'<a>' AS Bas_PlateletCount_Cellsx10_9,
			M_Plt.prefix AS Bas_PlateletCount_Accuracy, 
		ifnull(M_TBili.result_float, M_TBili_aux.result_float) AS Bas_BilirubinEntered,
			if(M_TBili.result_float IS NOT NULL, M_TBili.units, M_TBili_aux.units) AS Bas_Bilirubin_Units,
			'<a>' AS BAS_Bilirubin_Umol,
			if(M_TBili.result_float IS NOT NULL, M_TBili.prefix, M_TBili_aux.prefix) AS Bas_Bilirubin_Accuracy,	
		ifnull(M_lactate.result_float, M_lactate_aux.result_float) AS Bas_Lactate,
			if(M_lactate.result_float IS NOT NULL, M_lactate.units, M_lactate_aux.units) AS Bas_Lactate_Units,
			'<a>' AS BAS_Lactate_mmolL,
			if(M_lactate.result_float IS NOT NULL, M_lactate.prefix, M_lactate_aux.prefix) AS Bas_Lactate_Accuracy,		
		IFNULL(G.score, M_GCS.result_float) AS Bas_GlasgowComa, 
		M_ferritin.result_float AS Bas_Ferritin, 
			M_ferritin.units AS Bas_Ferritin_Accuracy, 
		M_Ddimer.result_float AS Bas_D_Dimer, 
			M_Ddimer.units AS Bas_D_Dimer_Units,
			'<a>' AS Bas_D_Dimer_ugL,
			M_Ddimer.prefix AS Bas_D_Dimer_Accuracy,
		M_CRP.result_float AS Bas_C_ReactiveProtein,
		 	M_CRP.units AS Bas_C_ReactiveProtein_Units,
		 	'<a>' AS Bas_C_ReactiveProtein_ugL,
		 	M_CRP.prefix AS Bas_C_ReactiveProtein_Accuracy,
		M_ANC.result_float AS Bas_NeutrophilCount, 
			M_ANC.units AS Bas_NeutrophilCount_Units,
			'<a>' AS Bas_NeutrophilCount_Cellsx10_9,
			M_ANC.prefix AS Bas_NeutrophilCount_Accuracy,
		M_lymphs.result_float AS Bas_LymphocyteCount, 
			M_lymphs.units AS Bas_LymphocyteCount_Units,
			'<a>' AS Bas_LymphocyteCount_Cellsx10_9,
			M_lymphs.prefix AS Bas_LymphocyteCount_Accuracy,
		'<a>' as Bas_TroponinTest,
			M_troponin.result_float AS Bas_TroponinResult, 
			M_troponin.units AS Bas_TroponinResult_Units,
			'<a>' AS Bas_TroponinResult_ngL,
			M_troponin.prefix AS Bas_TroponinResult_Accuracy,
			M_troponin.NORMAL_HIGH AS Bas_TroponinUpperLimit,
			M_troponin.units AS Bas_TroponinUpperLimit_Units,
			'<a>' AS Bas_TroponinUpperLimit_ngl,
		M_INR.result_float AS Bas_INR_or_PR,
			M_INR.prefix AS Bas_INR_or_PR_Accuracy,
		M_fibrinogen.result_float AS Bas_Fibrinogen, 
			M_fibrinogen.units AS Bas_Fibrinogen_Units,
			'<a>' AS `Bas_Fibrinogen_g/L`,
			M_fibrinogen.prefix AS Bas_Fibrinogen_Accuracy,
		M_temp.result_float AS Bas_Temperature,
			M_temp.units AS Bas_Temperature_Units,
			'<a>' AS Bas_Temperature_C,
		M_HR.result_float AS Bas_HeartRate,
		ifnull(M_sys.result_float, M_sys_aux.result_float) AS Bas_SystolicBloodPressure,
		M_RR.result_float AS Bas_RespiratoryRate,
		M_HCO3.result_float AS Bas_Bicarbonate,
			M_HCO3.units AS Bas_Bicarbonate_Units,
			'<a>' AS Bas_Bicarbonate_mEqL,
		M_albumin.result_float AS Bas_Albumin
	FROM REMAPe.ve3RandomizedSevere R
	LEFT JOIN REMAPe.ve2ApacheeScoreS A ON R.studypatientid = A.studypatientid
	LEFT JOIN REMAP.v3tempHypoxiaVar H ON R.studypatientid = H.studypatientid AND H.randomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBasSupp B ON R.studypatientid = B.studypatientid AND B.randomizationType = 'Severe'
	LEFT JOIN COVID_PHI.GCS_scores G ON R.studypatientid = G.studypatientid
	LEFT JOIN REMAP.v3tempBas M_Cr ON R.studypatientid = M_Cr.studypatientid AND M_Cr.baseline_standard = 'Cr' AND M_Cr.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_Cr_aux ON R.studypatientid = M_Cr_aux.studypatientid AND M_Cr_aux.baseline_standard = 'Cr_aux' AND M_Cr_aux.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_Plt ON R.studypatientid = M_Plt.studypatientid AND M_Plt.baseline_standard = 'Plt' AND M_Plt.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_TBili ON R.studypatientid = M_TBili.studypatientid AND M_TBili.baseline_standard = 'TBili' AND M_TBili.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_TBili_aux ON R.studypatientid = M_TBili_aux.studypatientid AND M_TBili_aux.baseline_standard = 'TBili_aux' AND M_TBili_aux.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_lactate ON R.studypatientid = M_lactate.studypatientid AND M_lactate.baseline_standard = 'lactate' AND M_lactate.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_lactate_aux ON R.studypatientid = M_lactate_aux.studypatientid AND M_lactate_aux.baseline_standard = 'lactate_aux' AND M_lactate_aux.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_GCS ON R.studypatientid = M_GCS.studypatientid AND M_GCS.baseline_standard = 'GCS' AND M_GCS.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_ferritin ON R.studypatientid = M_ferritin.studypatientid AND M_ferritin.baseline_standard = 'Ferritin' AND M_ferritin.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_Ddimer ON R.studypatientid = M_Ddimer.studypatientid AND M_Ddimer.baseline_standard = 'D-dimer' AND M_Ddimer.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_CRP ON R.studypatientid = M_CRP.studypatientid AND M_CRP.baseline_standard = 'C-reactive protein' AND M_CRP.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_ANC ON R.studypatientid = M_ANC.studypatientid AND M_ANC.baseline_standard = 'Neutrophil count' AND M_ANC.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_lymphs ON R.studypatientid = M_lymphs.studypatientid AND M_lymphs.baseline_standard = 'Lymphocyte count' AND M_lymphs.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_troponin ON R.studypatientid = M_troponin.studypatientid AND M_troponin.baseline_standard = 'Troponin' AND M_troponin.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_INR ON R.studypatientid = M_INR.studypatientid AND M_INR.baseline_standard = 'INR' AND M_INR.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_fibrinogen ON R.studypatientid = M_fibrinogen.studypatientid AND M_fibrinogen.baseline_standard = 'Fibrinogen' AND M_fibrinogen.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_temp ON R.studypatientid = M_temp.studypatientid AND M_temp.baseline_standard = 'Temp' AND M_temp.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_HR ON R.studypatientid = M_HR.studypatientid AND M_HR.baseline_standard = 'Heart rate' AND M_HR.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_sys ON R.studypatientid = M_sys.studypatientid AND M_sys.baseline_standard = 'BP_sys' AND M_sys.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_sys_aux ON R.studypatientid = M_sys_aux.studypatientid AND M_sys_aux.baseline_standard = 'BP_sys_aux' AND M_sys_aux.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_RR ON R.studypatientid = M_RR.studypatientid AND M_RR.baseline_standard = 'Respiratory rate' AND M_RR.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_HCO3 ON R.studypatientid = M_HCO3.studypatientid AND M_HCO3.baseline_standard = 'HCO3' AND M_HCO3.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_albumin ON R.studypatientid = M_albumin.studypatientid AND M_albumin.baseline_standard = 'Albumin' AND M_albumin.RandomizationType = 'Severe'	
	ORDER BY aux_RandomizationType, StudyPatientID
	;
	DROP TABLE REMAP.v3tempBas;
	DROP TABLE REMAP.v3tempHypoxiaVar;
	DROP TABLE REMAP.v3tempBasSupp;
SELECT * FROM REMAPe.ve3_Form2Baseline_sections5to7;
