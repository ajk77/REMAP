/*
REMAP-v3-reportingForms.sql
created by ajk77.github.io | @ajk77onX

NAVIGATION: 
	TABLE BUILD ORDER 
	REMAP.v3_RAR_condensed -> FROM REMAP.v3Lab, REMAP.v3Participant, REMAP.v3CalculatedStateHypoxiaAtEnroll, REMAP.v3RandomizedModerate, REMAP.v3LocOrder, REMAP.v3RandomizedSevere, REMAP.v3IcuDaysOnSupport
	REMAP.v3_Form2Baseline_sections5to7 -> FROM REMAP.v3Lab, REMAP.v3Physio, REMAP.v3RandomizedModerate, REMAP.v3RandomizedSevere, REMAP.v3CalculatedPEEPjoinFiO2, REMAP.v3CalculatedHourlyFiO2, REMAP.v3CalculatedStateHypoxiaAtEnroll, REMAP.v3OrganSupportInstance, COVID_PHI.v2ApacheeScoreS, REMAP.v3RRTInstance, REMAP.v3CalculatedSOFA
	REMAP.v3_Form4Daily_all -> REMAP.v3StudyDay, REMAP.v3IcuStay, REMAP.v3UnitStay, REMAP.v3OrganSupportInstance, REMAP.v3RRTInstance, REMAP.v3CalculatedSOFA, REMAP.v3PhysioStr, REMAP.v3CalculatedPFratio, REMAP.v3CalculatedHourlyFiO2, REMAP.v3CalculatedPEEPjoinFiO2
	REMAP.v3Day14Outcomes -> REMAP.v3StudyDay, REMAP.v3RandomizedSevere, REMAP.v3RandomizedModerate, REMAP.v3OrganSupportInstance, REMAP.v3RRTInstance, REMAP.v3SupplementalOxygenInstance, REMAP.v3Hospitalization
*/


/* v3_RAR_all */ 
DROP TABLE REMAP.v3_RAR_condensed;
	CREATE TABLE REMAP.v3_RAR_condensed
		WITH all_ddimer_24hrPreScreen AS (
			SELECT L.event_id, L.StudyPatientID, L.event_utc, L.result_float, L.units, L.NORMAL_HIGH 
			FROM
				(SELECT * FROM REMAP.v3Lab where sub_standard_meaning = 'D-dimer') as L 
				JOIN REMAP.v3Participant R ON L.StudyPatientID = R.StudyPatientID 
					AND L.event_utc BETWEEN ADDDATE(R.screendate_utc, INTERVAL -24 HOUR) AND R.screendate_utc
		), ddimer_closest AS (
			SELECT a.event_id, a.StudyPatientID, a.event_utc, a.result_float, a.units, a.NORMAL_HIGH
			FROM all_ddimer_24hrPreScreen a
			INNER JOIN (
			    SELECT StudyPatientID, MAX(event_utc) AS event_utc
			    FROM all_ddimer_24hrPreScreen
			    GROUP BY StudyPatientID
			) b ON a.StudyPatientID = b.StudyPatientID AND a.event_utc = b.event_utc
		), StateHypoxia_at_Randomization AS (
			SELECT StudyPatientID, MAX(StateHypoxia) AS StateHypoxia 
			FROM REMAP.v3CalculatedStateHypoxiaAtEnroll 
			WHERE RandomizationType = 'Moderate' 
			GROUP BY StudyPatientID
			UNION
			SELECT StudyPatientID, MAX(StateHypoxia) AS StateHypoxia 
			FROM REMAP.v3CalculatedStateHypoxiaAtEnroll 
			WHERE RandomizationType = 'Severe' 
				AND StudyPatientID NOT IN (
					SELECT StudyPatientID FROM REMAP.v3RandomizedModerate
				)
			GROUP BY StudyPatientID
		), last_location AS (
			SELECT IM.StudyPatientID, REMAP.to_local(MAX(REMAP.to_utc(EA.DISCH_DT_TM))) AS EndOfHospitalization_local
			FROM CT_DATA.ENCOUNTER_ALL EA 
			JOIN REMAP.v3IdMap IM ON EA.encntr_id = IM.ENCNTR_ID
			GROUP BY IM.StudyPatientID
		), outcomesDay21M AS (
			SELECT R.StudyPatientID, IFNULL(ROUND((504-I.hours_on_support_M)/24, 0), 22) AS ModerateOutcomeDay21
			FROM REMAP.v3RandomizedModerate R
			LEFT JOIN REMAP.v3IcuDaysOnSupport I ON R.StudyPatientID = I.StudyPatientID		
		), outcomesDay21S AS (
			SELECT R.StudyPatientID, IFNULL(ROUND((504-I.hours_on_support_S)/24, 0), 22) AS SevereOutcomeDay21 
			FROM REMAP.v3RandomizedSevere R
			LEFT JOIN REMAP.v3IcuDaysOnSupport I ON R.StudyPatientID = I.StudyPatientID		
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
			if(D.result_float IS NULL, 0, if(D.result_float < (REMAP.to_float(D.NORMAL_HIGH)*2), 1, 2)) AS DDimerCat,
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
		FROM REMAP.v3Participant P
			LEFT JOIN REMAP.v3RandomizedModerate M ON P.StudyPatientID = M.StudyPatientID
			LEFT JOIN REMAP.v3RandomizedSevere S ON P.StudyPatientID = S.StudyPatientID
			LEFT JOIN StateHypoxia_at_Randomization H ON P.StudyPatientID = H.StudyPatientID
			LEFT JOIN ddimer_closest D ON P.StudyPatientID = D.StudyPatientID
			LEFT JOIN last_location L ON P.StudyPatientID = L.StudyPatientID
			LEFT JOIN outcomesDay21M OM ON P.StudyPatientID = OM.StudyPatientID
			LEFT JOIN outcomesDay21S OS ON P.StudyPatientID = OS.StudyPatientID
		ORDER BY StudyPatientID
; 
SELECT * FROM REMAP.v3_RAR_condensed;


/* v3_Form2Baseline_sections5to7 */ 
DROP TABLE REMAP.v3_Form2Baseline_sections5to7;
CREATE TABLE REMAP.v3tempBas
		WITH all_sec6_meas AS (
			SELECT *, REMAP.to_baseline_standard(sub_standard_meaning) AS baseline_standard 
			FROM REMAP.v3Lab 
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
			FROM REMAP.v3Physio 
			WHERE sub_standard_meaning IN (
				'Glasgow Coma Score (total)'
			) 		
		), all_sec7_meas AS (
			SELECT *, REMAP.to_baseline_standard(sub_standard_meaning) AS baseline_standard 
			FROM REMAP.v3Lab where sub_standard_meaning IN (
				'Ferritin',	'D-dimer', 'C-reactive protein', 'Neutrophil count', 'Lymphocyte count', 
				'Troponin T','Troponin I (iStat)','Troponin I','Troponin (unknown)','Troponin (comment)',
				'INR','INR (comment)',
				'Fibrinogen', 'Temperature',
				'Bicarbonate','Bicarbonate (iStat)', 
				'Albumin'
			)
			UNION 
			SELECT *, REMAP.to_baseline_standard(sub_standard_meaning) AS baseline_standard 
			FROM REMAP.v3Physio 
			WHERE sub_standard_meaning IN (
				'Blood pressure (arterial systolic)', 'Blood pressure (systolic)', 'Heart rate',
				'Respiratory rate', 'Temperature', 'Temperature (conversion)', 'Temperature (metric)') 		
		), sec6_preRand AS (
			SELECT 'Moderate' as RandomizationType, R.randomized_utc, M.*
			FROM REMAP.v3RandomizedModerate R
			JOIN all_sec6_meas M ON R.StudyPatientID = M.StudyPatientID 
				AND M.event_utc < ADDDATE(R.randomized_utc, INTERVAL 2 HOUR) 
			UNION
			SELECT 'Severe' as RandomizationType, R.randomized_utc, M.*
			FROM REMAP.v3RandomizedSevere R
			JOIN all_sec6_meas M ON R.StudyPatientID = M.StudyPatientID 
				AND M.event_utc < ADDDATE(R.randomized_utc, INTERVAL 2 HOUR) 			
		), sec7_preRand AS (
			SELECT 'Moderate' as RandomizationType, R.randomized_utc, M.*
			FROM REMAP.v3RandomizedModerate R
			JOIN all_sec7_meas M ON R.StudyPatientID = M.StudyPatientID 
				AND M.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -8 HOUR) AND ADDDATE(R.randomized_utc, INTERVAL 2 HOUR) 
			UNION
			SELECT 'Severe' as RandomizationType, R.randomized_utc, M.*
	 		FROM REMAP.v3RandomizedSevere R
			JOIN all_sec7_meas M ON R.StudyPatientID = M.StudyPatientID 
				AND M.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -8 HOUR) AND ADDDATE(R.randomized_utc, INTERVAL 2 HOUR)
		), closest_meas_pre AS (  # can contain duplucate values at same event_utc
			SELECT a.*
			FROM sec6_preRand a
			INNER JOIN (
			    SELECT StudyPatientID, RandomizationType, baseline_standard,
				 	MIN(ABS(TIMESTAMPDIFF(SECOND, event_utc, randomized_utc))) AS smallest_event_diff
			    FROM sec6_preRand
			    GROUP BY StudyPatientID, RandomizationType, baseline_standard
			) AS b ON a.StudyPatientID = b.StudyPatientID 
				AND a.RandomizationType = b.RandomizationType 
				AND a.baseline_standard = b.baseline_standard
				AND ABS(TIMESTAMPDIFF(SECOND, a.event_utc, a.randomized_utc)) = b.smallest_event_diff
			UNION 
			SELECT a.*
			FROM sec7_preRand a
			INNER JOIN (
			    SELECT StudyPatientID, RandomizationType, baseline_standard,
				 	MIN(ABS(TIMESTAMPDIFF(SECOND, event_utc, randomized_utc))) AS smallest_event_diff
			    FROM sec7_preRand
			    GROUP BY StudyPatientID, RandomizationType, baseline_standard
			) AS b ON a.StudyPatientID = b.StudyPatientID 
				AND a.RandomizationType = b.RandomizationType 
				AND a.baseline_standard = b.baseline_standard
				AND ABS(TIMESTAMPDIFF(SECOND, a.event_utc, a.randomized_utc)) = b.smallest_event_diff 
		)#, closest_meas AS (  # grab max value at given event_utc
			SELECT a.*
			FROM closest_meas_pre a
			INNER JOIN (
			    SELECT StudyPatientID, RandomizationType, baseline_standard,
				 	MAX(result_float) AS max_result_float
			    FROM closest_meas_pre
			    GROUP BY StudyPatientID, RandomizationType, baseline_standard
			) AS b ON a.StudyPatientID = b.StudyPatientID 
				AND a.RandomizationType = b.RandomizationType 
				AND a.baseline_standard = b.baseline_standard
				AND a.result_float = b.max_result_float
	;
	CREATE TABLE REMAP.v3tempHypoxiaVar
		WITH PEEPjoinFiO2_preRand AS (
			SELECT 'Moderate' as RandomizationType, R.randomized_utc, C.*
			FROM REMAP.v3RandomizedModerate R
			JOIN REMAP.v3CalculatedPEEPjoinFiO2 C ON R.StudyPatientID = C.StudyPatientID 
				AND C.PEEP_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc
			UNION
			SELECT 'Severe' as RandomizationType, R.randomized_utc, C.*
			FROM REMAP.v3RandomizedSevere R
			JOIN REMAP.v3CalculatedPEEPjoinFiO2 C ON R.StudyPatientID = C.StudyPatientID 
				AND C.PEEP_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc 
		), FiO2_preRand AS (
			SELECT 'Moderate' as RandomizationType, R.randomized_utc, 
				C.StudyPatientID, C.event_utc AS FiO2_utc, C.result_float AS FiO2_float 
			FROM REMAP.v3RandomizedModerate R
			JOIN REMAP.v3CalculatedHourlyFiO2 C ON R.StudyPatientID = C.StudyPatientID 
				AND C.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc
			UNION
			SELECT 'Severe' as RandomizationType, R.randomized_utc, 
				C.StudyPatientID, C.event_utc AS FiO2_utc, C.result_float AS FiO2_float 
			FROM REMAP.v3RandomizedSevere R
			JOIN REMAP.v3CalculatedHourlyFiO2 C ON R.StudyPatientID = C.StudyPatientID 
				AND C.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc 
		), closest_PEEPjoinFiO2_pre AS (
			SELECT a.*
			FROM PEEPjoinFiO2_preRand a
			INNER JOIN (
			    SELECT StudyPatientID, RandomizationType, 
				 	MIN(ABS(TIMESTAMPDIFF(SECOND, PEEP_utc, randomized_utc))) AS smallest_event_diff
			    FROM PEEPjoinFiO2_preRand
			    GROUP BY StudyPatientID, RandomizationType
			) AS b ON a.StudyPatientID = b.StudyPatientID 
				AND a.RandomizationType = b.RandomizationType 
				AND ABS(TIMESTAMPDIFF(SECOND, a.PEEP_utc, a.randomized_utc)) = b.smallest_event_diff
		), closest_FiO2_preRand AS (
			SELECT a.*
			FROM FiO2_preRand a
			INNER JOIN (
			    SELECT StudyPatientID, RandomizationType, 
				 	MIN(ABS(TIMESTAMPDIFF(SECOND, FiO2_utc, randomized_utc))) AS smallest_event_diff
			    FROM FiO2_preRand
			    GROUP BY StudyPatientID, RandomizationType
			) AS b ON a.StudyPatientID = b.StudyPatientID 
				AND a.RandomizationType = b.RandomizationType 
				AND ABS(TIMESTAMPDIFF(SECOND, a.FiO2_utc, a.randomized_utc)) = b.smallest_event_diff
		), combined_stateHypoxia AS (
			SELECT R.StudyPatientID, 'Moderate' AS RandomizationType, H.onInvasiveVent,
				PaO2_float,	PaO2_units, PEEP_float, FiO2_float, PF_ratio, StateHypoxia
			FROM REMAP.v3RandomizedModerate R
			LEFT JOIN REMAP.v3CalculatedStateHypoxiaAtEnroll H 
			ON R.StudyPatientID = H.StudyPatientID AND H.RandomizationType = 'Moderate'
			UNION
			SELECT R.StudyPatientID, 'Severe' AS RandomizationType, H.onInvasiveVent,
				PaO2_float,	PaO2_units, PEEP_float, FiO2_float, PF_ratio, StateHypoxia  
			FROM REMAP.v3RandomizedSevere R 
			LEFT JOIN REMAP.v3CalculatedStateHypoxiaAtEnroll H 
			ON R.StudyPatientID = H.StudyPatientID AND H.RandomizationType = 'Severe'
		), hypoxia_meas AS (
			SELECT 
				H.StudyPatientID, H.RandomizationType,
				H.onInvasiveVent AS IV_atRand,
				H.PaO2_float, H.PaO2_units, 
				IFNULL(H.FiO2_float, IFNULL(E.FiO2_float, F.FiO2_float)) AS FiO2_float,
				IFNULL(H.PEEP_float, E.PEEP_float) AS PEEP_float,
				H.PF_ratio, H.StateHypoxia
			FROM combined_stateHypoxia H
			LEFT JOIN closest_PEEPjoinFiO2_pre E ON H.StudyPatientID = E.StudyPatientID AND H.RandomizationType = E.RandomizationType
			LEFT JOIN closest_FiO2_preRand F ON H.StudyPatientID = F.StudyPatientID AND H.RandomizationType = F.RandomizationType
		), HFNC_atRand AS (
			SELECT StudyPatientID, 'Moderate' AS RandomizationType, if(StudyPatientID IN (
				SELECT DISTINCT R.StudyPatientID
				FROM REMAP.v3RandomizedModerate R
				LEFT JOIN REMAP.v3OrganSupportInstance O 
				ON R.StudyPatientID = O.StudyPatientID
				WHERE O.support_type = 'HFNC'
					AND O.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc 
				), 1, 0) AS HFNC_atRand
			FROM REMAP.v3RandomizedModerate
			UNION 
			SELECT StudyPatientID, 'Severe' AS RandomizationType, if(StudyPatientID IN (
				SELECT DISTINCT R.StudyPatientID
				FROM REMAP.v3RandomizedSevere R
				LEFT JOIN REMAP.v3OrganSupportInstance O 
				ON R.StudyPatientID = O.StudyPatientID
				WHERE O.support_type = 'HFNC'
					AND O.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc 
				), 1, 0) AS HFNC_atRand
			FROM REMAP.v3RandomizedSevere
		), NIV_atRand AS (
			SELECT StudyPatientID, 'Moderate' AS RandomizationType, if(StudyPatientID IN (
				SELECT DISTINCT R.StudyPatientID
				FROM REMAP.v3RandomizedModerate R
				LEFT JOIN REMAP.v3OrganSupportInstance O 
				ON R.StudyPatientID = O.StudyPatientID
				WHERE O.support_type = 'NIV'
					AND O.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc 
				), 1, 0) AS NIV_atRand
			FROM REMAP.v3RandomizedModerate
			UNION 
			SELECT StudyPatientID, 'Severe' AS RandomizationType, if(StudyPatientID IN (
				SELECT DISTINCT R.StudyPatientID
				FROM REMAP.v3RandomizedSevere R
				LEFT JOIN REMAP.v3OrganSupportInstance O 
				ON R.StudyPatientID = O.StudyPatientID
				WHERE O.support_type = 'NIV'
					AND O.event_utc BETWEEN ADDDATE(R.randomized_utc, INTERVAL -24 HOUR) AND R.randomized_utc 
				), 1, 0) AS NIV_atRand
			FROM REMAP.v3RandomizedSevere
		), corrected_hypoxia_meas AS ( # because PEEP depends on organ support type
			SELECT 
				H.StudyPatientID, H.RandomizationType, H.PaO2_float, H.PaO2_units, H.FiO2_float,
				if(H.IV_atRand = 1 OR N.NIV_atRand = 1, H.PEEP_float, if(C.HFNC_atRand = 1, 0, NULL)) AS PEEP_float,
				H.PF_ratio, H.StateHypoxia
			FROM hypoxia_meas H
			LEFT JOIN HFNC_atRand C ON H.StudyPatientID = C.StudyPatientID AND H.RandomizationType = C.RandomizationType
			LEFT JOIN NIV_atRand N ON H.StudyPatientID = N.StudyPatientID AND H.RandomizationType = N.RandomizationType
		)
		SELECT * FROM corrected_hypoxia_meas
	;
	CREATE TABLE REMAP.v3tempBasSupp
		SELECT R.StudyPatientID, 'Moderate' AS RandomizationType, IFNULL(Bas_CardioSOFA, 0) AS Bas_CardioSOFA, 
			IF(T.StudyPatientID IS NULL, 'No', 'Yes') AS Bas_RRT, 
			IF(O.StudyPatientID IS NULL, 'No', 'Yes') AS Bas_ExtracorporealGas, 
			IF(O.StudyPatientID IS NULL, '', 'X') AS Bas_ECMO, '' AS Bas_ECCO2R
		FROM REMAP.v3RandomizedModerate R
		LEFT JOIN REMAP.v3RRTInstance T ON R.StudyPatientID = T.StudyPatientID AND T.event_utc <= R.randomized_utc
		LEFT JOIN REMAP.v3OrganSupportInstance O ON R.StudyPatientID = O.StudyPatientID AND O.support_type = 'ECMO' AND O.event_utc <= R.randomized_utc
		LEFT JOIN (
			SELECT R.StudyPatientID, MAX(score) AS Bas_CardioSOFA
			FROM REMAP.v3RandomizedModerate R
			LEFT JOIN REMAP.v3CalculatedSOFA C ON R.StudyPatientID = C.StudyPatientID 
			WHERE C.STUDY_DAY < 1
			GROUP BY R.StudyPatientID	
		) AS SOFA ON R.StudyPatientID = SOFA.StudyPatientID
		UNION
		SELECT R.StudyPatientID, 'Severe' AS RandomizationType, Bas_CardioSOFA, 
			IF(T.StudyPatientID IS NULL, 'No', 'Yes') AS Bas_RRT, 
			IF(O.StudyPatientID IS NULL, 'No', 'Yes') AS Bas_ExtracorporealGas, 
			IF(O.StudyPatientID IS NULL, '', 'X') AS Bas_ECMO, '' AS Bas_ECCO2R
		FROM REMAP.v3RandomizedSevere R
		LEFT JOIN REMAP.v3RRTInstance T ON R.StudyPatientID = T.StudyPatientID AND T.event_utc <= R.randomized_utc
		LEFT JOIN REMAP.v3OrganSupportInstance O ON R.StudyPatientID = O.StudyPatientID AND O.support_type = 'ECMO' AND O.event_utc <= R.randomized_utc
		LEFT JOIN (
			SELECT R.StudyPatientID, MAX(score) AS Bas_CardioSOFA
			FROM REMAP.v3RandomizedSevere R
			LEFT JOIN REMAP.v3CalculatedSOFA C ON R.StudyPatientID = C.StudyPatientID 
			WHERE C.STUDY_DAY < 1
			GROUP BY R.StudyPatientID	
		) AS SOFA ON R.StudyPatientID = SOFA.StudyPatientID	
	;
CREATE TABLE REMAP.v3_Form2Baseline_sections5to7
	SELECT 
		R.StudyPatientID, 'Moderate' AS aux_RandomizationType, CURRENT_TIMESTAMP as aux_last_update,
		'n/a' as Bas_APACHEScore,
		H.FiO2_float AS Bas_FIO2,
		H.PaO2_float AS Bas_PaO2Entered,
		H.PaO2_units AS Bas_PaO2Units,
		'<e>' AS Bas_PaO2_mmHg,
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
			'<e>' AS Bas_Creatinine_mmolL,
			if(M_Cr.result_float IS NOT NULL, M_Cr.prefix, M_Cr_aux.prefix) AS Bas_Creatinine_Accuracy,
		M_Plt.result_float AS Bas_PlateletCount, 
			M_Plt.units AS Bas_PlateletCount_Units,
			'<e>' AS Bas_PlateletCount_Cellsx10_9,
			M_Plt.prefix AS Bas_PlateletCount_Accuracy, 
		ifnull(M_TBili.result_float, M_TBili_aux.result_float) AS Bas_BilirubinEntered,
			if(M_TBili.result_float IS NOT NULL, M_TBili.units, M_TBili_aux.units) AS Bas_Bilirubin_Units,
			'<e>' AS BAS_Bilirubin_Umol,
			if(M_TBili.result_float IS NOT NULL, M_TBili.prefix, M_TBili_aux.prefix) AS Bas_Bilirubin_Accuracy,	
		ifnull(M_lactate.result_float, M_lactate_aux.result_float) AS Bas_Lactate,
			if(M_lactate.result_float IS NOT NULL, M_lactate.units, M_lactate_aux.units) AS Bas_Lactate_Units,
			'<e>' AS BAS_Lactate_mmolL,
			if(M_lactate.result_float IS NOT NULL, M_lactate.prefix, M_lactate_aux.prefix) AS Bas_Lactate_Accuracy,		
		IFNULL(G.score, M_GCS.result_float) AS Bas_GlasgowComa, 
		M_ferritin.result_float AS Bas_Ferritin, 
			M_ferritin.units AS Bas_Ferritin_Units, 
		M_Ddimer.result_float AS Bas_D_Dimer, 
			M_Ddimer.units AS Bas_D_Dimer_Units,
			'<e>' AS Bas_D_Dimer_ugL,
			M_Ddimer.prefix AS Bas_D_Dimer_Accuracy,
		M_CRP.result_float AS Bas_C_ReactiveProtein,
		 	M_CRP.units AS Bas_C_ReactiveProtein_Units,
		 	'<e>' AS Bas_C_ReactiveProtein_ugL,
		 	M_CRP.prefix AS Bas_C_ReactiveProtein_Accuracy,
		M_ANC.result_float AS Bas_NeutrophilCount, 
			M_ANC.units AS Bas_NeutrophilCount_Units,
			'<e>' AS Bas_NeutrophilCount_Cellsx10_9,
			M_ANC.prefix AS Bas_NeutrophilCount_Accuracy,
		M_lymphs.result_float AS Bas_LymphocyteCount, 
			M_lymphs.units AS Bas_LymphocyteCount_Units,
			'<e>' AS Bas_LymphocyteCount_Cellsx10_9,
			M_lymphs.prefix AS Bas_LymphocyteCount_Accuracy,
		'<a>' as Bas_TroponinTest,
			M_troponin.result_float AS Bas_TroponinResult, 
			M_troponin.units AS Bas_TroponinResult_Units,
			'<e>' AS Bas_TroponinResult_ngL,
			M_troponin.prefix AS Bas_TroponinResult_Accuracy,
			M_troponin.NORMAL_HIGH AS Bas_TroponinUpperLimit,
			M_troponin.units AS Bas_TroponinUpperLimit_Units,
			'<e>' AS Bas_TroponinUpperLimit_ngl,
		M_INR.result_float AS Bas_INR_or_PR,
			M_INR.prefix AS Bas_INR_or_PR_Accuracy,
		M_fibrinogen.result_float AS Bas_Fibrinogen, 
			M_fibrinogen.units AS Bas_Fibrinogen_Units,
			'<e>' AS `Bas_Fibrinogen_g/L`,
			M_fibrinogen.prefix AS Bas_Fibrinogen_Accuracy,
		M_temp.result_float AS Bas_Temperature,
			M_temp.units AS Bas_Temperature_Units,
			'<e>' AS Bas_Temperature_C,
		M_HR.result_float AS Bas_HeartRate,
		ifnull(M_sys.result_float, M_sys_aux.result_float) AS Bas_SystolicBloodPressure,
		M_RR.result_float AS Bas_RespiratoryRate,
		M_HCO3.result_float AS Bas_Bicarbonate,
			M_HCO3.units AS Bas_Bicarbonate_Units,
			'<e>' AS Bas_Bicarbonate_mEqL,
		M_albumin.result_float AS Bas_Albumin
	FROM REMAP.v3RandomizedModerate R
	LEFT JOIN REMAP.v3tempHypoxiaVar H ON R.StudyPatientID = H.StudyPatientID AND H.randomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBasSupp B ON R.StudyPatientID = B.StudyPatientID AND B.randomizationType = 'Moderate'
	LEFT JOIN COVID_PHI.GCS_scores G ON R.StudyPatientID = G.StudyPatientID
	LEFT JOIN REMAP.v3tempBas M_Cr ON R.StudyPatientID = M_Cr.StudyPatientID AND M_Cr.baseline_standard = 'Cr' AND M_Cr.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_Cr_aux ON R.StudyPatientID = M_Cr_aux.StudyPatientID AND M_Cr_aux.baseline_standard = 'Cr_aux' AND M_Cr_aux.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_Plt ON R.StudyPatientID = M_Plt.StudyPatientID AND M_Plt.baseline_standard = 'Plt' AND M_Plt.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_TBili ON R.StudyPatientID = M_TBili.StudyPatientID AND M_TBili.baseline_standard = 'TBili' AND M_TBili.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_TBili_aux ON R.StudyPatientID = M_TBili_aux.StudyPatientID AND M_TBili_aux.baseline_standard = 'TBili_aux' AND M_TBili_aux.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_lactate ON R.StudyPatientID = M_lactate.StudyPatientID AND M_lactate.baseline_standard = 'lactate' AND M_lactate.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_lactate_aux ON R.StudyPatientID = M_lactate_aux.StudyPatientID AND M_lactate_aux.baseline_standard = 'lactate_aux' AND M_lactate_aux.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_GCS ON R.StudyPatientID = M_GCS.StudyPatientID AND M_GCS.baseline_standard = 'GCS' AND M_GCS.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_ferritin ON R.StudyPatientID = M_ferritin.StudyPatientID AND M_ferritin.baseline_standard = 'Ferritin' AND M_ferritin.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_Ddimer ON R.StudyPatientID = M_Ddimer.StudyPatientID AND M_Ddimer.baseline_standard = 'D-dimer' AND M_Ddimer.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_CRP ON R.StudyPatientID = M_CRP.StudyPatientID AND M_CRP.baseline_standard = 'C-reactive protein' AND M_CRP.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_ANC ON R.StudyPatientID = M_ANC.StudyPatientID AND M_ANC.baseline_standard = 'Neutrophil count' AND M_ANC.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_lymphs ON R.StudyPatientID = M_lymphs.StudyPatientID AND M_lymphs.baseline_standard = 'Lymphocyte count' AND M_lymphs.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_troponin ON R.StudyPatientID = M_troponin.StudyPatientID AND M_troponin.baseline_standard = 'Troponin' AND M_troponin.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_INR ON R.StudyPatientID = M_INR.StudyPatientID AND M_INR.baseline_standard = 'INR' AND M_INR.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_fibrinogen ON R.StudyPatientID = M_fibrinogen.StudyPatientID AND M_fibrinogen.baseline_standard = 'Fibrinogen' AND M_fibrinogen.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_temp ON R.StudyPatientID = M_temp.StudyPatientID AND M_temp.baseline_standard = 'Temp' AND M_temp.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_HR ON R.StudyPatientID = M_HR.StudyPatientID AND M_HR.baseline_standard = 'Heart rate' AND M_HR.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_sys ON R.StudyPatientID = M_sys.StudyPatientID AND M_sys.baseline_standard = 'BP_sys' AND M_sys.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_sys_aux ON R.StudyPatientID = M_sys_aux.StudyPatientID AND M_sys_aux.baseline_standard = 'BP_sys_aux' AND M_sys_aux.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_RR ON R.StudyPatientID = M_RR.StudyPatientID AND M_RR.baseline_standard = 'Respiratory rate' AND M_RR.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_HCO3 ON R.StudyPatientID = M_HCO3.StudyPatientID AND M_HCO3.baseline_standard = 'HCO3' AND M_HCO3.RandomizationType = 'Moderate'
	LEFT JOIN REMAP.v3tempBas M_albumin ON R.StudyPatientID = M_albumin.StudyPatientID AND M_albumin.baseline_standard = 'Albumin' AND M_albumin.RandomizationType = 'Moderate'
UNION  
	SELECT 
		R.StudyPatientID, 'Severe' AS aux_RandomizationType, CURRENT_TIMESTAMP as aux_last_update,
		A.apachee_APS as Bas_APACHEScore,
		H.FiO2_float AS Bas_FIO2,
		H.PaO2_float AS Bas_PaO2Entered,
		H.PaO2_units AS Bas_PaO2Units,
		'<e>' AS Bas_PaO2_mmHg,
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
			'<e>' AS Bas_Creatinine_mmolL,
			if(M_Cr.result_float IS NOT NULL, M_Cr.prefix, M_Cr_aux.prefix) AS Bas_Creatinine_Accuracy,
		M_Plt.result_float AS Bas_PlateletCount, 
			M_Plt.units AS Bas_PlateletCount_Units,
			'<e>' AS Bas_PlateletCount_Cellsx10_9,
			M_Plt.prefix AS Bas_PlateletCount_Accuracy, 
		ifnull(M_TBili.result_float, M_TBili_aux.result_float) AS Bas_BilirubinEntered,
			if(M_TBili.result_float IS NOT NULL, M_TBili.units, M_TBili_aux.units) AS Bas_Bilirubin_Units,
			'<e>' AS BAS_Bilirubin_Umol,
			if(M_TBili.result_float IS NOT NULL, M_TBili.prefix, M_TBili_aux.prefix) AS Bas_Bilirubin_Accuracy,	
		ifnull(M_lactate.result_float, M_lactate_aux.result_float) AS Bas_Lactate,
			if(M_lactate.result_float IS NOT NULL, M_lactate.units, M_lactate_aux.units) AS Bas_Lactate_Units,
			'<e>' AS BAS_Lactate_mmolL,
			if(M_lactate.result_float IS NOT NULL, M_lactate.prefix, M_lactate_aux.prefix) AS Bas_Lactate_Accuracy,		
		IFNULL(G.score, M_GCS.result_float) AS Bas_GlasgowComa, 
		M_ferritin.result_float AS Bas_Ferritin, 
			M_ferritin.units AS Bas_Ferritin_Units, 
		M_Ddimer.result_float AS Bas_D_Dimer, 
			M_Ddimer.units AS Bas_D_Dimer_Units,
			'<e>' AS Bas_D_Dimer_ugL,
			M_Ddimer.prefix AS Bas_D_Dimer_Accuracy,
		M_CRP.result_float AS Bas_C_ReactiveProtein,
		 	M_CRP.units AS Bas_C_ReactiveProtein_Units,
		 	'<e>' AS Bas_C_ReactiveProtein_ugL,
		 	M_CRP.prefix AS Bas_C_ReactiveProtein_Accuracy,
		M_ANC.result_float AS Bas_NeutrophilCount, 
			M_ANC.units AS Bas_NeutrophilCount_Units,
			'<e>' AS Bas_NeutrophilCount_Cellsx10_9,
			M_ANC.prefix AS Bas_NeutrophilCount_Accuracy,
		M_lymphs.result_float AS Bas_LymphocyteCount, 
			M_lymphs.units AS Bas_LymphocyteCount_Units,
			'<e>' AS Bas_LymphocyteCount_Cellsx10_9,
			M_lymphs.prefix AS Bas_LymphocyteCount_Accuracy,
		'<a>' as Bas_TroponinTest,
			M_troponin.result_float AS Bas_TroponinResult, 
			M_troponin.units AS Bas_TroponinResult_Units,
			'<e>' AS Bas_TroponinResult_ngL,
			M_troponin.prefix AS Bas_TroponinResult_Accuracy,
			M_troponin.NORMAL_HIGH AS Bas_TroponinUpperLimit,
			M_troponin.units AS Bas_TroponinUpperLimit_Units,
			'<e>' AS Bas_TroponinUpperLimit_ngl,
		M_INR.result_float AS Bas_INR_or_PR,
			M_INR.prefix AS Bas_INR_or_PR_Accuracy,
		M_fibrinogen.result_float AS Bas_Fibrinogen, 
			M_fibrinogen.units AS Bas_Fibrinogen_Units,
			'<e>' AS `Bas_Fibrinogen_g/L`,
			M_fibrinogen.prefix AS Bas_Fibrinogen_Accuracy,
		M_temp.result_float AS Bas_Temperature,
			M_temp.units AS Bas_Temperature_Units,
			'<e>' AS Bas_Temperature_C,
		M_HR.result_float AS Bas_HeartRate,
		ifnull(M_sys.result_float, M_sys_aux.result_float) AS Bas_SystolicBloodPressure,
		M_RR.result_float AS Bas_RespiratoryRate,
		M_HCO3.result_float AS Bas_Bicarbonate,
			M_HCO3.units AS Bas_Bicarbonate_Units,
			'<e>' AS Bas_Bicarbonate_mEqL,
		M_albumin.result_float AS Bas_Albumin
	FROM REMAP.v3RandomizedSevere R
	LEFT JOIN COVID_PHI.v2ApacheeScoreS A ON R.StudyPatientID = A.StudyPatientID
	LEFT JOIN REMAP.v3tempHypoxiaVar H ON R.StudyPatientID = H.StudyPatientID AND H.randomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBasSupp B ON R.StudyPatientID = B.StudyPatientID AND B.randomizationType = 'Severe'
	LEFT JOIN COVID_PHI.GCS_scores G ON R.StudyPatientID = G.StudyPatientID
	LEFT JOIN REMAP.v3tempBas M_Cr ON R.StudyPatientID = M_Cr.StudyPatientID AND M_Cr.baseline_standard = 'Cr' AND M_Cr.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_Cr_aux ON R.StudyPatientID = M_Cr_aux.StudyPatientID AND M_Cr_aux.baseline_standard = 'Cr_aux' AND M_Cr_aux.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_Plt ON R.StudyPatientID = M_Plt.StudyPatientID AND M_Plt.baseline_standard = 'Plt' AND M_Plt.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_TBili ON R.StudyPatientID = M_TBili.StudyPatientID AND M_TBili.baseline_standard = 'TBili' AND M_TBili.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_TBili_aux ON R.StudyPatientID = M_TBili_aux.StudyPatientID AND M_TBili_aux.baseline_standard = 'TBili_aux' AND M_TBili_aux.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_lactate ON R.StudyPatientID = M_lactate.StudyPatientID AND M_lactate.baseline_standard = 'lactate' AND M_lactate.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_lactate_aux ON R.StudyPatientID = M_lactate_aux.StudyPatientID AND M_lactate_aux.baseline_standard = 'lactate_aux' AND M_lactate_aux.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_GCS ON R.StudyPatientID = M_GCS.StudyPatientID AND M_GCS.baseline_standard = 'GCS' AND M_GCS.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_ferritin ON R.StudyPatientID = M_ferritin.StudyPatientID AND M_ferritin.baseline_standard = 'Ferritin' AND M_ferritin.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_Ddimer ON R.StudyPatientID = M_Ddimer.StudyPatientID AND M_Ddimer.baseline_standard = 'D-dimer' AND M_Ddimer.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_CRP ON R.StudyPatientID = M_CRP.StudyPatientID AND M_CRP.baseline_standard = 'C-reactive protein' AND M_CRP.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_ANC ON R.StudyPatientID = M_ANC.StudyPatientID AND M_ANC.baseline_standard = 'Neutrophil count' AND M_ANC.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_lymphs ON R.StudyPatientID = M_lymphs.StudyPatientID AND M_lymphs.baseline_standard = 'Lymphocyte count' AND M_lymphs.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_troponin ON R.StudyPatientID = M_troponin.StudyPatientID AND M_troponin.baseline_standard = 'Troponin' AND M_troponin.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_INR ON R.StudyPatientID = M_INR.StudyPatientID AND M_INR.baseline_standard = 'INR' AND M_INR.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_fibrinogen ON R.StudyPatientID = M_fibrinogen.StudyPatientID AND M_fibrinogen.baseline_standard = 'Fibrinogen' AND M_fibrinogen.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_temp ON R.StudyPatientID = M_temp.StudyPatientID AND M_temp.baseline_standard = 'Temp' AND M_temp.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_HR ON R.StudyPatientID = M_HR.StudyPatientID AND M_HR.baseline_standard = 'Heart rate' AND M_HR.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_sys ON R.StudyPatientID = M_sys.StudyPatientID AND M_sys.baseline_standard = 'BP_sys' AND M_sys.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_sys_aux ON R.StudyPatientID = M_sys_aux.StudyPatientID AND M_sys_aux.baseline_standard = 'BP_sys_aux' AND M_sys_aux.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_RR ON R.StudyPatientID = M_RR.StudyPatientID AND M_RR.baseline_standard = 'Respiratory rate' AND M_RR.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_HCO3 ON R.StudyPatientID = M_HCO3.StudyPatientID AND M_HCO3.baseline_standard = 'HCO3' AND M_HCO3.RandomizationType = 'Severe'
	LEFT JOIN REMAP.v3tempBas M_albumin ON R.StudyPatientID = M_albumin.StudyPatientID AND M_albumin.baseline_standard = 'Albumin' AND M_albumin.RandomizationType = 'Severe'	
	ORDER BY aux_RandomizationType, StudyPatientID
	;
	DROP TABLE REMAP.v3tempBas;
	DROP TABLE REMAP.v3tempHypoxiaVar;
	DROP TABLE REMAP.v3tempBasSupp;
SELECT * FROM REMAP.v3_Form2Baseline_sections5to7;

### v3_Form4Daily_all ### 
DROP TABLE REMAP.v3_Form4Daily_all;
CREATE TABLE REMAP.v3_Form4Daily_all
	WITH study_days AS (
		SELECT SD.* 
		FROM REMAP.v3StudyDay SD
		JOIN REMAP.v3IcuStay I 
		ON SD.StudyPatientID = I.StudyPatientID 
			AND SD.day_start_utc <= I.end_utc 
			AND SD.day_end_utc >= I.beg_utc
	), pt_loc_pre AS ( 
		SELECT SD.StudyPatientID, SD.STUDY_DAY, SD.RandomizationType, GROUP_CONCAT(DISTINCT U.unit_type) AS pt_loc_str
		FROM study_days SD
		JOIN REMAP.v3UnitStay U
		ON SD.StudyPatientID = U.StudyPatientID
			AND SD.day_start_utc <= U.end_utc 
			AND SD.day_end_utc >= U.beg_utc
		GROUP BY SD.StudyPatientID, SD.STUDY_DAY, SD.RandomizationType
	), pt_loc AS (
		SELECT StudyPatientID, STUDY_DAY, RandomizationType, pt_loc_str AS aux_pt_loc_str,
			IF(pt_loc_str LIKE 'ICU%' OR pt_loc_str LIKE '%,ICU%', 'Physical', 'Repurposed') AS Physical_ICU
	 	FROM pt_loc_pre
	), support_pre AS (
		SELECT SD.StudyPatientID, SD.STUDY_DAY, SD.RandomizationType, GROUP_CONCAT(DISTINCT O.support_type) AS support_str
		FROM study_days SD
		LEFT JOIN REMAP.v3OrganSupportInstance O
		ON SD.StudyPatientID = O.StudyPatientID
			AND O.event_utc BETWEEN SD.day_start_utc AND SD.day_end_utc
		GROUP BY SD.StudyPatientID, SD.STUDY_DAY, SD.RandomizationType
	), binary_support AS (
		SELECT StudyPatientID, STUDY_DAY, RandomizationType, support_str AS aux_support_str,
			IF(support_str LIKE '%HFNC%', 'Yes', 'No') AS on_HFNC,
			IF(support_str LIKE '%NIV%', 'Yes', 'No') AS on_NIV,
			IF(support_str LIKE '%ECMO%', 'Yes', 'No') AS on_ECMO
	 	FROM support_pre
	), rrt_support AS (
		SELECT DISTINCT SD.StudyPatientID, SD.STUDY_DAY, SD.RandomizationType, if(R.StudyPatientID IS NOT NULL, 'Yes', 'No') AS on_RRT
		FROM study_days SD
		LEFT JOIN REMAP.v3RRTInstance R
		ON SD.StudyPatientID = R.StudyPatientID
			AND R.event_utc BETWEEN SD.day_start_utc AND SD.day_end_utc
	), sofa AS (	
		SELECT SD.StudyPatientID, SD.Study_day, SD.RandomizationType, IFNULL(MAX(C.score), 0) AS CardioSOFA
		FROM study_days SD
		LEFT JOIN REMAP.v3CalculatedSOFA C 
		ON SD.StudyPatientID = C.StudyPatientID 
			AND SD.study_day = C.study_day
		GROUP BY SD.StudyPatientID, SD.Study_day, SD.RandomizationType
	), IMV AS (
		SELECT SD.StudyPatientID, SD.STUDY_DAY, SD.RandomizationType, 
			IFNULL(LEAST(timestampdiff(HOUR, MIN(O.event_utc),  MAX(O.event_utc)), 24), 0) as IMV_hours
		FROM study_days SD
		LEFT JOIN REMAP.v3OrganSupportInstance O
		ON SD.StudyPatientID = O.StudyPatientID
			AND O.event_utc BETWEEN SD.day_start_utc AND SD.day_end_utc
		WHERE O.support_type = 'IMV'
		GROUP BY SD.StudyPatientID, SD.STUDY_DAY, SD.RandomizationType
	), airway_doc_pre AS (
		SELECT P.StudyPatientID, SD.STUDY_DAY, SD.RandomizationType,  if(P.documented_text LIKE '%Tracheostomy%', 'Tracheostomy', 'Endotracheal Tube') AS pt_airway
		FROM REMAP.v3PhysioStr P
		JOIN study_days SD ON SD.StudyPatientID = P.StudyPatientID AND P.event_utc BETWEEN SD.day_start_utc AND SD.day_end_utc
		WHERE P.sub_standard_meaning = 'Airway' OR P.result_str = 'IV device' 
	), airway_doc AS (
		SELECT StudyPatientID, Study_day, RandomizationType, GROUP_CONCAT(DISTINCT pt_airway) AS airway_str
		FROM airway_doc_pre SD 
		GROUP BY StudyPatientID, Study_day, RandomizationType
	), oxygenation_pre AS ( # find lowest pf ratio 
		SELECT SD.StudyPatientID, SD.Study_day, SD.RandomizationType, PF_ratio, PaO2_float, FiO2_float, PEEP_float, PaO2_utc
		FROM study_days SD
		LEFT JOIN REMAP.v3CalculatedPFratio C 
		ON SD.StudyPatientID = C.StudyPatientID 
			AND C.PaO2_utc BETWEEN SD.day_start_utc AND SD.day_end_utc
	), min_pf AS (
		SELECT StudyPatientID, Study_day, randomizationType, MIN(PF_ratio) AS min_pf
		FROM oxygenation_pre
		GROUP BY StudyPatientID, Study_day, RandomizationType
	), oxygenation AS (
		SELECT O.StudyPatientID, O.Study_day, O.RandomizationType, O.PF_ratio, O.PaO2_float, O.FiO2_float, 
			MAX(O.PEEP_float) AS PEEP_float, O.PaO2_utc
		FROM oxygenation_pre O
		JOIN min_pf M ON O.StudyPatientID = M.StudyPatientID AND O.study_day = M.study_day AND O.randomizationType = M.randomizationType
		WHERE O.pf_ratio = M.min_pf
		GROUP BY O.StudyPatientID, O.Study_day, O.RandomizationType, O.PF_ratio, O.PaO2_float, O.FiO2_float, O.PaO2_utc 
	), FiO2_pre AS ( # find highest fio2
		SELECT SD.StudyPatientID, SD.Study_day, SD.RandomizationType, result_float AS FiO2_float
		FROM study_days SD
		LEFT JOIN REMAP.v3CalculatedHourlyFiO2 C 
		ON SD.StudyPatientID = C.StudyPatientID 
			AND C.event_utc BETWEEN SD.day_start_utc AND SD.day_end_utc
	), max_fio2 AS (
		SELECT StudyPatientID, Study_day, randomizationType, MAX(FiO2_float) AS max_fio2
		FROM FiO2_pre
		GROUP BY StudyPatientID, Study_day, RandomizationType
	), peep_pre AS ( # find peep corresponding to fio2
		SELECT SD.StudyPatientID, SD.Study_day, SD.RandomizationType, PEEP_float, FiO2_float
		FROM study_days SD
		LEFT JOIN REMAP.v3CalculatedPEEPjoinFiO2 C 
		ON SD.StudyPatientID = C.StudyPatientID 
			AND C.FiO2_utc BETWEEN SD.day_start_utc AND SD.day_end_utc
	), fio2_peep AS (
		SELECT F.StudyPatientID, F.Study_day, F.RandomizationType, F.max_fio2 AS FiO2_float, MAX(P.PEEP_float) AS PEEP_float
		FROM max_fio2 F
		LEFT JOIN peep_pre P ON P.StudyPatientID = F.StudyPatientID AND P.study_day = F.study_day AND P.randomizationType = F.randomizationType
		WHERE P.fio2_float = F.max_fio2 OR P.fio2_float IS NULL 
		GROUP BY F.StudyPatientID, F.Study_day, F.RandomizationType, F.max_fio2 
	)
	SELECT DISTINCT 
		SD.StudyPatientID,
		SD.RandomizationType,
		SD.study_day,
		SD.day_date_local,
		'Yes'AS pt_in_ICU,
		P.Physical_ICU,
		P.aux_pt_loc_str,
		A.airway_str,
		B.on_HFNC,
		B.on_NIV,
		IFNULL(I.IMV_hours, 0) AS IMV_hours,
		IFNULL(O.FiO2_float, E.FiO2_float) AS FiO2,
		O.PaO2_float AS PaO2,
		IFNULL(O.PEEP_float, E.FiO2_float) AS PEEP, 
		S.CardioSOFA,
		R.on_RRT,
		B.on_ECMO,
		if(B.on_ECMO IS NOT NULL, 'ECMO', NULL) AS ECMO_type
	FROM study_days SD
	LEFT JOIN pt_loc P ON SD.StudyPatientID = P.StudyPatientID AND SD.STUDY_DAY = P.STUDY_DAY AND SD.RandomizationType = P.RandomizationType
	LEFT JOIN binary_support B ON SD.StudyPatientID = B.StudyPatientID AND SD.STUDY_DAY = B.STUDY_DAY AND SD.RandomizationType = B.RandomizationType
	LEFT JOIN rrt_support R ON SD.StudyPatientID = R.StudyPatientID AND SD.STUDY_DAY = R.STUDY_DAY AND SD.RandomizationType = R.RandomizationType
	LEFT JOIN sofa S ON SD.StudyPatientID = S.StudyPatientID AND SD.STUDY_DAY = S.STUDY_DAY AND SD.RandomizationType = S.RandomizationType
	LEFT JOIN IMV I ON SD.StudyPatientID = I.StudyPatientID AND SD.STUDY_DAY = I.STUDY_DAY AND SD.RandomizationType = I.RandomizationType
	LEFT JOIN airway_doc A ON SD.StudyPatientID = A.StudyPatientID AND SD.STUDY_DAY = A.STUDY_DAY AND SD.RandomizationType = A.RandomizationType
	LEFT JOIN oxygenation O ON SD.StudyPatientID = O.StudyPatientID AND SD.STUDY_DAY = O.STUDY_DAY AND SD.RandomizationType = O.RandomizationType
	LEFT JOIN fio2_peep E ON SD.StudyPatientID = E.StudyPatientID AND SD.STUDY_DAY = E.STUDY_DAY AND SD.RandomizationType = E.RandomizationType
	ORDER BY SD.StudyPatientID, SD.RandomizationType, SD.Study_Day
; 
SELECT * FROM REMAP.v3_Form4Daily_all;

DROP TABLE REMAP.v3Day14Outcomes;
CREATE TABLE REMAP.v3Day14Outcomes
WITH day_14 AS (
	SELECT 
		SD.StudyPatientID AS StudyPatientID,
		SD.STUDY_DAY AS STUDY_DAY,
		SD.day_date_local AS day_date_local,
		SD.day_start_utc AS day_start_utc,
		SD.day_end_utc AS day_end_utc,
		SD.RandomizationType AS RandomizationType,
		'First' AS randomization_count
	FROM (
		REMAP.v3StudyDay SD
		JOIN (
			SELECT REMAP.v3RandomizedSevere.StudyPatientID AS StudyPatientID
			FROM REMAP.v3RandomizedSevere
			WHERE REMAP.v3RandomizedSevere.StudyPatientID in (
				SELECT REMAP.v3RandomizedModerate.StudyPatientID
				FROM REMAP.v3RandomizedModerate) IS FALSE) R ON SD.StudyPatientID = R.StudyPatientID )
	WHERE ((SD.STUDY_DAY = 14) AND (SD.RandomizationType = 'Severe'))
	UNION
	SELECT 
		SD.StudyPatientID AS StudyPatientID,
		SD.STUDY_DAY AS STUDY_DAY,
		SD.day_date_local AS day_date_local,
		SD.day_start_utc AS day_start_utc,
		SD.day_end_utc AS day_end_utc,
		SD.RandomizationType AS RandomizationType,
		'First' AS randomization_count
	FROM (
		REMAP.v3StudyDay SD
		JOIN REMAP.v3RandomizedModerate R ON SD.StudyPatientID = R.StudyPatientID)
	WHERE ((SD.STUDY_DAY = 14) AND (SD.RandomizationType = 'Moderate')) 
	UNION
	SELECT 
		SD.StudyPatientID AS StudyPatientID,
		SD.STUDY_DAY AS STUDY_DAY,
		SD.day_date_local AS day_date_local,
		SD.day_start_utc AS day_start_utc,
		SD.day_end_utc AS day_end_utc,
		SD.RandomizationType AS RandomizationType,
		'Last' AS randomization_count
	FROM (
		REMAP.v3StudyDay SD
		JOIN (
			SELECT REMAP.v3RandomizedSevere.StudyPatientID AS StudyPatientID
			FROM REMAP.v3RandomizedSevere
			WHERE REMAP.v3RandomizedSevere.StudyPatientID in (
				SELECT REMAP.v3RandomizedModerate.StudyPatientID
				FROM REMAP.v3RandomizedModerate)
		) R ON SD.StudyPatientID = R.StudyPatientID)
	WHERE ((SD.STUDY_DAY = 14) AND (SD.RandomizationType = 'Severe'))
	), with_ranges AS (
	SELECT 
		day_14.StudyPatientID AS StudyPatientID,
		day_14.day_start_utc AS day_14_start_utc,
		day_14.day_end_utc AS day_14_end_utc,
		day_14.randomization_count AS randomization_count,
		day_14.RandomizationType AS RandomizationType,
		day_14.day_date_local AS day_date_local,
		day_14.STUDY_DAY AS STUDY_DAY
	FROM day_14
	), has_ECMO AS (
		SELECT F.StudyPatientID AS StudyPatientID,F.randomization_count AS randomization_count,1 AS has_ECMO
		FROM (REMAP.v3OrganSupportInstance O
		JOIN with_ranges F ON(((O.StudyPatientID = F.StudyPatientID) AND (O.event_utc BETWEEN F.day_14_start_utc AND F.day_14_end_utc))))
		WHERE (O.support_type = 'ECMO')
		GROUP BY O.StudyPatientID,F.randomization_count
	), has_IMV AS (
		SELECT F.StudyPatientID AS StudyPatientID,F.randomization_count AS randomization_count,1 AS has_IMV
		FROM (REMAP.v3OrganSupportInstance O
		JOIN with_ranges F ON(((O.StudyPatientID = F.StudyPatientID) AND (O.event_utc BETWEEN F.day_14_start_utc AND F.day_14_end_utc))))
		WHERE (O.support_type = 'IMV')
		GROUP BY O.StudyPatientID,F.randomization_count
	), has_NIV AS (
		SELECT F.StudyPatientID AS StudyPatientID,F.randomization_count AS randomization_count,1 AS has_NIV
		FROM (REMAP.v3OrganSupportInstance O
		JOIN with_ranges F ON(((O.StudyPatientID = F.StudyPatientID) AND (O.event_utc BETWEEN F.day_14_start_utc AND F.day_14_end_utc))))
		WHERE (O.support_type = 'NIV')
		GROUP BY O.StudyPatientID,F.randomization_count
	), has_Vaso AS (
		SELECT F.StudyPatientID AS StudyPatientID,F.randomization_count AS randomization_count,1 AS has_Vaso
		FROM (REMAP.v3OrganSupportInstance O
		JOIN with_ranges F ON(((O.StudyPatientID = F.StudyPatientID) AND (O.event_utc BETWEEN F.day_14_start_utc AND F.day_14_end_utc))))
		WHERE (O.support_type = 'Vasopressor')
		GROUP BY O.StudyPatientID,F.randomization_count
	), has_RRT AS (
		SELECT F.StudyPatientID AS StudyPatientID,F.randomization_count AS randomization_count,1 AS has_RRT
		FROM (REMAP.v3RRTInstance O
		JOIN with_ranges F ON(((O.StudyPatientID = F.StudyPatientID) AND (O.event_utc BETWEEN F.day_14_start_utc AND F.day_14_end_utc))))
		GROUP BY O.StudyPatientID,F.randomization_count
	), has_relaxedHF AS (
		SELECT F.StudyPatientID AS StudyPatientID,F.randomization_count AS randomization_count,1 AS has_relaxedHF
		FROM (REMAP.v3SupplementalOxygenInstance O
		JOIN with_ranges F ON(((O.StudyPatientID = F.StudyPatientID) AND (O.event_utc BETWEEN F.day_14_start_utc AND F.day_14_end_utc))))
		WHERE (O.support_type = 'relaxedHF')
		GROUP BY O.StudyPatientID,F.randomization_count
	), has_suppO2 AS (
		SELECT F.StudyPatientID AS StudyPatientID,F.randomization_count AS randomization_count,1 AS has_suppO2
		FROM (REMAP.v3SupplementalOxygenInstance O
		JOIN with_ranges F ON(((O.StudyPatientID = F.StudyPatientID) AND (O.event_utc BETWEEN F.day_14_start_utc AND F.day_14_end_utc))))
		WHERE (O.support_type <> 'relaxedHF')
		GROUP BY O.StudyPatientID,F.randomization_count
	), deceasedBy14 AS (
		SELECT WR.StudyPatientID, WR.randomization_count, if(H.DeceasedAtDischarge = 'Yes', 1, 0) AS Deceased
		FROM REMAP.v3Hospitalization H
		JOIN with_ranges WR ON H.StudyPatientID = WR.StudyPatientID AND H.EndOfHospitalization_utc <= WR.day_14_end_utc
		WHERE H.DeceasedAtDischarge = 'Yes'
	), pre_result AS (
		SELECT 
			W.StudyPatientID AS StudyPatientID,
			W.day_14_start_utc AS day_14_start_utc,
			W.day_14_end_utc AS day_14_end_utc,
			W.randomization_count AS randomization_count,
			W.RandomizationType AS RandomizationType,
			W.day_date_local AS day_date_local,
			W.STUDY_DAY AS STUDY_DAY,
			S.has_suppO2 AS Hosp_low_flow_O2,
			if(((N.has_NIV = 1) OR (H.has_relaxedHF = 1)),1, NULL) AS has_NIVorRelaxedHF,
			I.has_IMV AS has_IMV,
			if(((I.has_IMV = 1) AND ((V.has_Vaso = 1) OR (R.has_RRT = 1))),1, NULL) AS has_IMVplus,
			E.has_ECMO AS has_ECMO,
			D.Deceased
		FROM 
			with_ranges W
			LEFT JOIN has_ECMO E ON W.StudyPatientID = E.StudyPatientID AND W.randomization_count = E.randomization_count
			LEFT JOIN has_IMV I ON W.StudyPatientID = I.StudyPatientID AND W.randomization_count = I.randomization_count
			LEFT JOIN has_NIV N ON W.StudyPatientID = N.StudyPatientID AND W.randomization_count = N.randomization_count
			LEFT JOIN has_Vaso V ON W.StudyPatientID = V.StudyPatientID AND W.randomization_count = V.randomization_count
			LEFT JOIN has_RRT R ON W.StudyPatientID = R.StudyPatientID AND W.randomization_count = R.randomization_count
			LEFT JOIN has_relaxedHF H ON W.StudyPatientID = H.StudyPatientID AND W.randomization_count = H.randomization_count
			LEFT JOIN has_suppO2 S ON W.StudyPatientID = S.StudyPatientID AND W.randomization_count = S.randomization_count
			LEFT JOIN deceasedBy14 D ON W.StudyPatientID = D.StudyPatientID AND W.randomization_count = D.randomization_count
	)
	SELECT pre_result.StudyPatientID AS StudyPatientID, 
		pre_result.randomization_count AS RandomizationSequence,
		if(((pre_result.Hosp_low_flow_O2 IS NULL) 
			AND (pre_result.has_NIVorRelaxedHF IS NULL) 
			AND (pre_result.has_IMV IS NULL) 
			AND (pre_result.has_IMVplus IS NULL) 
			AND (pre_result.has_ECMO IS NULL)),
			1,0) AS Hosp_no_supp_oxygen, 
		IFNULL(pre_result.Hosp_low_flow_O2,0) AS Hosp_low_flow,
		IFNULL(pre_result.has_NIVorRelaxedHF,0) AS Non_invasive_vent, 
		IFNULL(pre_result.has_IMV,0) AS Invasive_mech_vent, 
		IFNULL(pre_result.has_IMVplus,0) AS Inv_mech_vent_plus, 
		IFNULL(pre_result.has_ECMO,0) AS ECMO_,
		IFNULL(pre_result.Deceased,0) AS Deceased,
		0 AS Unknown_,
		pre_result.RandomizationType AS RandomizationType,
		pre_result.STUDY_DAY AS STUDY_DAY,
		pre_result.day_date_local AS day_date_local,
		pre_result.day_14_start_utc AS day_start_utc,
		pre_result.day_14_end_utc AS day_end_utc
	FROM pre_result
	ORDER BY pre_result.StudyPatientID, pre_result.randomization_count, pre_result.RandomizationType
; 
 Select * FROM REMAP.v3Day14Outcomes;


/* ************************************** future ************************************** */

### v3_Form6Discharge_all ###


