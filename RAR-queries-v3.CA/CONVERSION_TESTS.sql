
/* CA tests */
SELECT 'v3Participant' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3Participant
UNION
SELECT 'v3Participant', 'CA', COUNT(*) FROM REMAP_CA.v3Participant  
UNION
SELECT 'v3Participant', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3Participant ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3Participant noma 
					WHERE (ca.StudyPatientId=noma.StudyPatientID OR (ca.StudyPatientId IS NULL AND noma.StudyPatientID IS NULL)) AND
						(ca.screendate_utc=noma.screendate_utc OR (ca.screendate_utc IS NULL AND noma.screendate_utc IS NULL)) AND
						(ca.REGIMEN=noma.REGIMEN OR (ca.REGIMEN IS NULL AND noma.REGIMEN IS NULL)) AND
					#	(ca.MRN=noma.MRN OR (ca.MRN IS NULL AND noma.MRN IS NULL)) AND
						(ca.PERSON_ID=noma.PERSON_ID OR (ca.PERSON_ID IS NULL AND noma.PERSON_ID IS NULL)) AND
						(ca.source_system=noma.source_system OR (ca.source_system IS NULL AND noma.source_system IS NULL)) 											
				);	
		
	
SELECT 'v3IdMap' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3IdMap
UNION
SELECT 'v3IdMap', 'CA', COUNT(*) FROM REMAP_CA.v3IdMap  
UNION
SELECT 'v3IdMap', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3IdMap ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3IdMap noma 
					WHERE (ca.encntr_id=noma.encntr_id OR (ca.encntr_id IS NULL AND noma.encntr_id IS NULL)) AND
						(LPAD(ca.fin, 13, '0')=LPAD(noma.fin, 13, '0') OR (ca.fin IS NULL AND noma.fin IS NULL)) AND
						(ca.studypatientid=noma.studypatientid OR (ca.studypatientid IS NULL AND noma.studypatientid IS NULL)) AND
						(ca.encounter_type=noma.encounter_type OR (ca.encounter_type IS NULL AND noma.encounter_type IS NULL)) 											
				);	
						
	
SELECT 'v3LocOrder' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3LocOrder
UNION
SELECT 'v3LocOrder', 'CA', COUNT(*) FROM REMAP_CA.v3LocOrder  
UNION
SELECT 'v3LocOrder', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3LocOrder ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3LocOrder noma 
					WHERE (ca.STUDYPATIENTID=noma.STUDYPATIENTID OR (ca.STUDYPATIENTID IS NULL AND noma.STUDYPATIENTID IS NULL)) AND
						(ca.encntr_id=noma.encntr_id OR (ca.encntr_id IS NULL AND noma.encntr_id IS NULL)) #AND
						#(ca.beg_utc=noma.beg_utc OR (ca.beg_utc IS NULL AND noma.beg_utc IS NULL)) AND
						#(ca.end_utc=noma.end_utc OR (ca.end_utc IS NULL AND noma.end_utc IS NULL)) AND												
						#(ca.LOC_FACILITY_CD=noma.LOC_FACILITY_CD OR (ca.LOC_FACILITY_CD IS NULL AND noma.LOC_FACILITY_CD IS NULL)) AND
						#(ca.LOC_NURSE_UNIT_CD=noma.LOC_NURSE_UNIT_CD OR (ca.LOC_NURSE_UNIT_CD IS NULL AND noma.LOC_NURSE_UNIT_CD IS NULL)) AND
						#(ca.loc_order=noma.loc_order OR (ca.loc_order IS NULL AND noma.loc_order IS NULL)) AND
						#(ca.screening_location=noma.screening_location OR (ca.screening_location IS NULL AND noma.screening_location IS NULL)) AND
						#(ca.max_end_of_prior_loc_orders=noma.max_end_of_prior_loc_orders OR (ca.max_end_of_prior_loc_orders IS NULL AND noma.max_end_of_prior_loc_orders IS NULL)) 
					);	
												
		#SELECT * FROM  REMAP_CA.v3Lab WHERE studypatientid = 0400100017;
SELECT 'v3Lab' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3Lab
UNION
SELECT 'v3Lab', 'CA', COUNT(*) FROM REMAP_CA.v3Lab  
UNION
SELECT 'v3Lab', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3Lab ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3Lab noma 
					WHERE (ca.EVENT_ID=noma.EVENT_ID OR (ca.EVENT_ID IS NULL AND noma.EVENT_ID IS NULL)) AND
						(ca.STUDYPATIENTID=noma.STUDYPATIENTID OR (ca.STUDYPATIENTID IS NULL AND noma.STUDYPATIENTID IS NULL)) AND
						(ca.event_utc=noma.event_utc OR (ca.event_utc IS NULL AND noma.event_utc IS NULL)) AND
						(ca.sub_standard_meaning=noma.sub_standard_meaning OR (ca.sub_standard_meaning IS NULL AND noma.sub_standard_meaning IS NULL)) AND												
						(ca.prefix=noma.prefix OR (ca.prefix IS NULL AND noma.prefix IS NULL)) AND
						(ca.result_float=noma.result_float OR (ca.result_float IS NULL AND noma.result_float IS NULL)) AND
						(ca.units=noma.units OR (ca.units IS NULL AND noma.units IS NULL)) AND
						(ca.documented_val=noma.documented_val OR (ca.documented_val IS NULL AND noma.documented_val IS NULL)) AND
						(ca.NORMAL_LOW=noma.NORMAL_LOW OR (ca.NORMAL_LOW IS NULL AND noma.NORMAL_LOW IS NULL)) AND
						(ca.NORMAL_HIGH=noma.NORMAL_HIGH OR (ca.NORMAL_HIGH IS NULL AND noma.NORMAL_HIGH IS NULL)) 
				);	


SELECT 'v3Physio' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3Physio
UNION
SELECT 'v3Physio', 'CA', COUNT(*) FROM REMAP_CA.v3Physio  
UNION
SELECT 'v3Physio', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3Physio ca 
WHERE event_id IN (	SELECT DISTINCT event_id 
					FROM REMAP.v3Physio noma 
					#WHERE (ca.EVENT_ID=noma.EVENT_ID) #AND
						#(ca.STUDYPATIENTID=noma.STUDYPATIENTID OR (ca.STUDYPATIENTID IS NULL AND noma.STUDYPATIENTID IS NULL)) AND
						#(ca.event_utc=noma.event_utc OR (ca.event_utc IS NULL AND noma.event_utc IS NULL)) #AND
						#(ca.sub_standard_meaning=noma.sub_standard_meaning OR (ca.sub_standard_meaning IS NULL AND noma.sub_standard_meaning IS NULL)) AND												
						#(ca.prefix=noma.prefix OR (ca.prefix IS NULL AND noma.prefix IS NULL)) AND
						#(ca.result_float=noma.result_float OR (ca.result_float IS NULL AND noma.result_float IS NULL)) AND
						#(ca.units=noma.units OR (ca.units IS NULL AND noma.units IS NULL)) AND
						#(ca.documented_val=noma.documented_val OR (ca.documented_val IS NULL AND noma.documented_val IS NULL)) 
				);				
				
SELECT 'v3PhysioStr' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3PhysioStr
UNION
SELECT 'v3PhysioStr', 'CA', COUNT(*) FROM REMAP_CA.v3PhysioStr  
UNION
SELECT 'v3PhysioStr', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3PhysioStr ca 
WHERE event_id IN (	SELECT DISTINCT event_id
					FROM REMAP.v3PhysioStr noma 
					#WHERE ca.EVENT_ID=noma.EVENT_ID AND
				#		ca.STUDYPATIENTID=noma.STUDYPATIENTID AND
				#		ca.event_utc=noma.event_utc AND
				#		ca.sub_standard_meaning=noma.sub_standard_meaning AND												
					#	(ca.result_str=noma.result_str OR (ca.result_str IS NULL AND noma.result_str IS NULL)) AND
				#		ca.units=noma.units#AND
						#(ca.documented_text=noma.documented_text OR (ca.documented_text IS NULL AND noma.documented_text IS NULL)) 
				);	



SELECT 'v3IO' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3IO
UNION
SELECT 'v3IO', 'CA', COUNT(*) FROM REMAP_CA.v3IO 
UNION
SELECT 'v3IO', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3IO ca 
WHERE event_id IN (	SELECT DISTINCT event_id
					FROM REMAP.v3IO noma 
				#	WHERE (ca.EVENT_ID=noma.EVENT_ID OR (ca.EVENT_ID IS NULL AND noma.EVENT_ID IS NULL)) AND
				#		(ca.STUDYPATIENTID=noma.STUDYPATIENTID OR (ca.STUDYPATIENTID IS NULL AND noma.STUDYPATIENTID IS NULL)) AND
				#		(ca.event_utc=noma.event_utc OR (ca.event_utc IS NULL AND noma.event_utc IS NULL)) AND
				#		(ca.sub_standard_meaning=noma.sub_standard_meaning OR (ca.sub_standard_meaning IS NULL AND noma.sub_standard_meaning IS NULL)) AND												
				#		(ca.result_float=noma.result_float OR (ca.result_float IS NULL AND noma.result_float IS NULL)) AND
				#		(ca.units=noma.units OR (ca.units IS NULL AND noma.units IS NULL)) 
				);		
						
	
SELECT 'v3Med' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3Med
UNION
SELECT 'v3Med', 'CA', COUNT(*) FROM REMAP_CA.v3Med  
UNION
SELECT 'v3Med', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3Med ca 
WHERE event_id IN (	SELECT DISTINCT event_id 
					FROM REMAP.v3Med noma 
				#	WHERE (ca.EVENT_ID=noma.EVENT_ID OR (ca.EVENT_ID IS NULL AND noma.EVENT_ID IS NULL)) AND
				#		(ca.STUDYPATIENTID=noma.STUDYPATIENTID OR (ca.STUDYPATIENTID IS NULL AND noma.STUDYPATIENTID IS NULL)) AND
				#		(ca.event_utc=noma.event_utc OR (ca.event_utc IS NULL AND noma.event_utc IS NULL)) AND
				#		(ca.sub_standard_meaning=noma.sub_standard_meaning OR (ca.sub_standard_meaning IS NULL AND noma.sub_standard_meaning IS NULL)) AND												
				#		(ca.ADMIN_DOSAGE=noma.ADMIN_DOSAGE OR (ca.ADMIN_DOSAGE IS NULL AND noma.ADMIN_DOSAGE IS NULL)) AND
				#		(ca.units=noma.units OR (ca.units IS NULL AND noma.units IS NULL)) AND
				#		(ca.route=noma.route OR (ca.route IS NULL AND noma.route IS NULL)) 
				);	
			
SELECT 'v3OrganSupportInstance' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3OrganSupportInstance
UNION
SELECT 'v3OrganSupportInstance', 'CA', COUNT(*) FROM REMAP_CA.v3OrganSupportInstance  
UNION
SELECT 'v3OrganSupportInstance', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3OrganSupportInstance ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3OrganSupportInstance noma 
					WHERE ca.EVENT_ID=noma.EVENT_ID AND
						ca.STUDYPATIENTID=noma.STUDYPATIENTID AND
						ca.event_utc=noma.event_utc AND
						ca.support_type=noma.support_type #AND												
						#(ca.documented_source=noma.documented_source OR (ca.documented_source IS NULL AND noma.documented_source IS NULL)) 
				);	

SELECT * from REMAP_CA.v3RandomizedSevere  ;
SELECT * from REMAP.v3RandomizedSevere  ;

SELECT 'v3RandomizedSevere' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3RandomizedSevere
UNION
SELECT 'v3RandomizedSevere', 'CA', COUNT(*) FROM REMAP_CA.v3RandomizedSevere  
UNION
SELECT 'v3RandomizedSevere', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3RandomizedSevere ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3RandomizedSevere noma 
					WHERE (ca.STUDYPATIENTID=noma.STUDYPATIENTID OR (ca.STUDYPATIENTID IS NULL AND noma.STUDYPATIENTID IS NULL)) AND
						(ca.randomized_utc=noma.randomized_utc OR (ca.randomized_utc IS NULL AND noma.randomized_utc IS NULL)) 
				);	
					
						
SELECT 'v3RandomizedModerate' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3RandomizedModerate
UNION
SELECT 'v3RandomizedModerate', 'CA', COUNT(*) FROM REMAP_CA.v3RandomizedModerate  
UNION
SELECT 'v3RandomizedModerate', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3RandomizedModerate ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3RandomizedModerate noma 
					WHERE (ca.STUDYPATIENTID=noma.STUDYPATIENTID OR (ca.STUDYPATIENTID IS NULL AND noma.STUDYPATIENTID IS NULL)) AND
						(ca.randomized_utc=noma.randomized_utc OR (ca.randomized_utc IS NULL AND noma.randomized_utc IS NULL)) 
				);		
						

SELECT 'v3StudyDay' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3StudyDay
UNION
SELECT 'v3StudyDay', 'CA', COUNT(*) FROM REMAP_CA.v3StudyDay  
UNION
SELECT 'v3StudyDay', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3StudyDay ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3StudyDay noma 
					WHERE (ca.STUDYPATIENTID=noma.STUDYPATIENTID OR (ca.STUDYPATIENTID IS NULL AND noma.STUDYPATIENTID IS NULL)) AND
						(ca.STUDY_DAY=noma.STUDY_DAY OR (ca.STUDY_DAY IS NULL AND noma.STUDY_DAY IS NULL)) AND
						(ca.day_date_local=noma.day_date_local OR (ca.day_date_local IS NULL AND noma.day_date_local IS NULL)) AND
						(ca.day_start_utc=noma.day_start_utc OR (ca.day_start_utc IS NULL AND noma.day_start_utc IS NULL)) AND
						(ca.day_end_utc=noma.day_end_utc OR (ca.day_end_utc IS NULL AND noma.day_end_utc IS NULL)) AND
						(ca.RandomizationType=noma.RandomizationType OR (ca.RandomizationType IS NULL AND noma.RandomizationType IS NULL)) 
				);	
						
	
SELECT 'v3UnitStay' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3UnitStay
UNION
SELECT 'v3UnitStay', 'CA', COUNT(*) FROM REMAP_CA.v3UnitStay  
UNION
SELECT 'v3UnitStay', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3UnitStay ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3UnitStay noma 
					WHERE (ca.STUDYPATIENTID=noma.STUDYPATIENTID OR (ca.STUDYPATIENTID IS NULL AND noma.STUDYPATIENTID IS NULL)) AND
						(ca.unit_type=noma.unit_type OR (ca.unit_type IS NULL AND noma.unit_type IS NULL)) AND
						(ca.loc_order_unit_start=noma.loc_order_unit_start OR (ca.loc_order_unit_start IS NULL AND noma.loc_order_unit_start IS NULL)) AND
						(ca.loc_order_unit_end=noma.loc_order_unit_end OR (ca.loc_order_unit_end IS NULL AND noma.loc_order_unit_end IS NULL)) AND
						(ca.beg_utc=noma.beg_utc OR (ca.beg_utc IS NULL AND noma.beg_utc IS NULL)) AND
						(ca.end_utc=noma.end_utc OR (ca.end_utc IS NULL AND noma.end_utc IS NULL)) AND
						(ca.includes_organSupport=noma.includes_organSupport OR (ca.includes_organSupport IS NULL AND noma.includes_organSupport IS NULL)) AND
						(ca.includes_stepdownUnit=noma.includes_stepdownUnit OR (ca.includes_stepdownUnit IS NULL AND noma.includes_stepdownUnit IS NULL)) AND
						(ca.includes_ignoreUnit=noma.includes_ignoreUnit OR (ca.includes_ignoreUnit IS NULL AND noma.includes_ignoreUnit IS NULL)) AND
						(ca.includes_pandemicICU=noma.includes_pandemicICU OR (ca.includes_pandemicICU IS NULL AND noma.includes_pandemicICU IS NULL)) 
				);	


SELECT 'v3IcuStay' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3IcuStay
UNION
SELECT 'v3IcuStay', 'CA', COUNT(*) FROM REMAP_CA.v3IcuStay  
UNION
SELECT 'v3IcuStay', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3IcuStay ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3IcuStay noma 
					WHERE (ca.STUDYPATIENTID=noma.STUDYPATIENTID OR (ca.STUDYPATIENTID IS NULL AND noma.STUDYPATIENTID IS NULL)) AND
						(ca.stay_count=noma.stay_count OR (ca.stay_count IS NULL AND noma.stay_count IS NULL)) AND
						(ca.beg_utc=noma.beg_utc OR (ca.beg_utc IS NULL AND noma.beg_utc IS NULL)) AND
						(ca.end_utc=noma.end_utc OR (ca.end_utc IS NULL AND noma.end_utc IS NULL)) AND
						(ca.includes_organSupport=noma.includes_organSupport OR (ca.includes_organSupport IS NULL AND noma.includes_organSupport IS NULL)) AND
						(ca.includes_stepdownUnit=noma.includes_stepdownUnit OR (ca.includes_stepdownUnit IS NULL AND noma.includes_stepdownUnit IS NULL)) AND
						(ca.includes_ignoreUnit=noma.includes_ignoreUnit OR (ca.includes_ignoreUnit IS NULL AND noma.includes_ignoreUnit IS NULL)) AND
						(ca.includes_pandemicICU=noma.includes_pandemicICU OR (ca.includes_pandemicICU IS NULL AND noma.includes_pandemicICU IS NULL)) AND
						(ca.includes_EDUnit=noma.includes_EDUnit OR (ca.includes_EDUnit IS NULL AND noma.includes_EDUnit IS NULL)) AND
						(ca.beg_utc_is_organ_support_adjusted=noma.beg_utc_is_organ_support_adjusted OR (ca.beg_utc_is_organ_support_adjusted IS NULL AND noma.beg_utc_is_organ_support_adjusted IS NULL)) AND
						(ca.loc_start=noma.loc_start OR (ca.loc_start IS NULL AND noma.loc_start IS NULL)) AND
						(ca.loc_end=noma.loc_end OR (ca.loc_end IS NULL AND noma.loc_end IS NULL)) AND
						(ca.unit_type_list=noma.unit_type_list OR (ca.unit_type_list IS NULL AND noma.unit_type_list IS NULL)) 
				);	


		
SELECT 'v3RRTInstance' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3RRTInstance
UNION
SELECT 'v3RRTInstance', 'CA', COUNT(*) FROM REMAP_CA.v3RRTInstance  
UNION
SELECT 'v3RRTInstance', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3RRTInstance ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3RRTInstance noma 
					WHERE (ca.event_id=noma.event_id OR (ca.event_id IS NULL AND noma.event_id IS NULL)) AND
						(ca.studypatientid=noma.studypatientid OR (ca.studypatientid IS NULL AND noma.studypatientid IS NULL)) AND
						(ca.event_utc=noma.event_utc OR (ca.event_utc IS NULL AND noma.event_utc IS NULL)) AND
						(ca.support_type=noma.support_type OR (ca.support_type IS NULL AND noma.support_type IS NULL)) AND
						(ca.documented_source=noma.documented_source OR (ca.documented_source IS NULL AND noma.documented_source IS NULL)) 
				);	
						
						
		
SELECT 'v3SupplementalOxygenInstance' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3SupplementalOxygenInstance
UNION
SELECT 'v3SupplementalOxygenInstance', 'CA', COUNT(*) FROM REMAP_CA.v3SupplementalOxygenInstance  
UNION
SELECT 'v3SupplementalOxygenInstance', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3SupplementalOxygenInstance ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3SupplementalOxygenInstance noma 
					WHERE ca.event_id=noma.event_id AND
						ca.studypatientid=noma.studypatientid AND
						ca.event_utc=noma.event_utc  /*AND
						ca.support_type=noma.support_type*/
				);
						
  
	
SELECT 'v3CalculatedHourlyFiO2' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3CalculatedHourlyFiO2
UNION
SELECT 'v3CalculatedHourlyFiO2', 'CA', COUNT(*) FROM REMAP_CA.v3CalculatedHourlyFiO2  
UNION
SELECT 'v3CalculatedHourlyFiO2', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3CalculatedHourlyFiO2 ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3CalculatedHourlyFiO2 noma 
					WHERE ca.STUDYPATIENTID=noma.STUDYPATIENTID AND
						ca.event_utc=noma.event_utc AND
						ca.result_float=noma.result_float AND
						ca.fio2_source=noma.fio2_source 
				);
				

			
SELECT 'v3CalculatedPFratio' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3CalculatedPFratio
UNION
SELECT 'v3CalculatedPFratio', 'CA', COUNT(*) FROM REMAP_CA.v3CalculatedPFratio  
UNION
SELECT 'v3CalculatedPFratio', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3CalculatedPFratio ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3CalculatedPFratio noma 
					WHERE ca.STUDYPATIENTID=noma.STUDYPATIENTID AND
						(ca.PF_ratio=noma.PF_ratio OR (ca.PF_ratio IS NULL AND noma.PF_ratio IS NULL)) AND
						(ca.PaO2_float=noma.PaO2_float OR (ca.PaO2_float IS NULL AND noma.PaO2_float IS NULL)) AND
						(ca.PaO2_units=noma.PaO2_units OR (ca.PaO2_units IS NULL AND noma.PaO2_units IS NULL)) AND
						(ca.PaO2_utc=noma.PaO2_utc OR (ca.PaO2_utc IS NULL AND noma.PaO2_utc IS NULL)) AND
						(ca.FiO2_float=noma.FiO2_float OR (ca.FiO2_float IS NULL AND noma.FiO2_float IS NULL)) AND
						(ca.FiO2_utc=noma.FiO2_utc OR (ca.FiO2_utc IS NULL AND noma.FiO2_utc IS NULL)) AND
						(ca.PEEP_float=noma.PEEP_float OR (ca.PEEP_float IS NULL AND noma.PEEP_float IS NULL)) AND
						(ca.PEEP_utc=noma.PEEP_utc  OR (ca.PEEP_utc IS NULL AND noma.PEEP_utc IS NULL)) 
				);
						

SELECT 'v3CalculatedPEEPjoinFiO2' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3CalculatedPEEPjoinFiO2
UNION
SELECT 'v3CalculatedPEEPjoinFiO2', 'CA', COUNT(*) FROM REMAP_CA.v3CalculatedPEEPjoinFiO2  
UNION
SELECT 'v3CalculatedPEEPjoinFiO2', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3CalculatedPEEPjoinFiO2 ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3CalculatedPEEPjoinFiO2 noma 
					WHERE ca.STUDYPATIENTID=noma.STUDYPATIENTID AND
						(ca.FiO2_float=noma.FiO2_float OR (ca.FiO2_float IS NULL AND noma.FiO2_float IS NULL)) AND
						(ca.FiO2_utc=noma.FiO2_utc OR (ca.FiO2_float IS NULL AND noma.FiO2_float IS NULL)) AND
						(ca.PEEP_float=noma.PEEP_float OR (ca.PEEP_float IS NULL AND noma.PEEP_float IS NULL)) AND
						(ca.PEEP_utc=noma.PEEP_utc  OR (ca.PEEP_utc IS NULL AND noma.PEEP_utc IS NULL)) 
				);		
		

SELECT 'v3CalculatedStateHypoxiaAtEnroll' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3CalculatedStateHypoxiaAtEnroll
UNION
SELECT 'v3CalculatedStateHypoxiaAtEnroll', 'CA', COUNT(*) FROM REMAP_CA.v3CalculatedStateHypoxiaAtEnroll  
UNION
SELECT 'v3CalculatedStateHypoxiaAtEnroll', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3CalculatedStateHypoxiaAtEnroll ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3CalculatedStateHypoxiaAtEnroll noma 
					WHERE ca.STUDYPATIENTID=noma.STUDYPATIENTID AND
						ca.RandomizationType=noma.RandomizationType AND
						(ca.StateHypoxia=noma.StateHypoxia OR (ca.StateHypoxia IS NULL AND noma.StateHypoxia IS NULL)) AND
						(ca.onInvasiveVent=noma.onInvasiveVent OR (ca.onInvasiveVent IS NULL AND noma.onInvasiveVent IS NULL)) AND
						(ca.PaO2_float=noma.PaO2_float OR (ca.PaO2_float IS NULL AND noma.PaO2_float IS NULL)) AND
						(ca.PaO2_units=noma.PaO2_units OR (ca.PaO2_units IS NULL AND noma.PaO2_units IS NULL)) AND
						(ca.PaO2_utc=noma.PaO2_utc OR (ca.PaO2_utc IS NULL AND noma.PaO2_utc IS NULL)) AND
						(ca.PEEP_float=noma.PEEP_float OR (ca.PEEP_float IS NULL AND noma.PEEP_float IS NULL)) AND
						(ca.PEEP_utc=noma.PEEP_utc OR (ca.PEEP_utc IS NULL AND noma.PEEP_utc IS NULL)) AND
						(ca.PF_ratio=noma.PF_ratio OR (ca.PF_ratio IS NULL AND noma.PF_ratio IS NULL)) AND
						(ca.FiO2_float=noma.FiO2_float OR (ca.FiO2_float IS NULL AND noma.FiO2_float IS NULL)) AND
						ca.FiO2_utc=noma.FiO2_utc																								
				);			
	


SELECT 'v3CalculatedSOFA' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3CalculatedSOFA
UNION
SELECT 'v3CalculatedSOFA', 'CA', COUNT(*) FROM REMAP_CA.v3CalculatedSOFA  
UNION
SELECT 'v3CalculatedSOFA', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3CalculatedSOFA ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3CalculatedSOFA noma 
					WHERE ca.STUDYPATIENTID=noma.STUDYPATIENTID AND
						ca.study_day=noma.study_day AND
						(ca.score=noma.score OR (ca.score IS NULL AND noma.score IS NULL)) AND
						(ca.RandomizationType=noma.RandomizationType OR (ca.RandomizationType IS NULL AND noma.RandomizationType IS NULL))  
				);				
		

SELECT 'v3Hospitalization' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3Hospitalization
UNION
SELECT 'v3Hospitalization', 'CA', COUNT(*) FROM REMAP_CA.v3Hospitalization  
UNION
SELECT 'v3Hospitalization', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3Hospitalization ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3Hospitalization noma 
					WHERE ca.STUDYPATIENTID=noma.STUDYPATIENTID AND
						(ca.StartOfHospitalization_utc=noma.StartOfHospitalization_utc OR (ca.StartOfHospitalization_utc IS NULL AND noma.StartOfHospitalization_utc IS NULL)) AND
						(ca.EndOfHospitalization_utc=noma.EndOfHospitalization_utc OR (ca.EndOfHospitalization_utc IS NULL AND noma.EndOfHospitalization_utc IS NULL)) AND
						(ca.DeceasedAtDischarge=noma.DeceasedAtDischarge OR (ca.DeceasedAtDischarge IS NULL AND noma.DeceasedAtDischarge IS NULL)) 
				);		
				
  
SELECT 'v3IcuAdmitDaysOnSupport' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3IcuAdmitDaysOnSupport
UNION
SELECT 'v3IcuAdmitDaysOnSupport', 'CA', COUNT(*) FROM REMAP_CA.v3IcuAdmitDaysOnSupport  
UNION
SELECT 'v3IcuAdmitDaysOnSupport', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3IcuAdmitDaysOnSupport ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3IcuAdmitDaysOnSupport noma 
					WHERE (ca.minutes_on_support_M=noma.minutes_on_support_M OR (ca.minutes_on_support_M IS NULL AND noma.minutes_on_support_M IS NULL)) AND
						(ca.minutes_on_support_S=noma.minutes_on_support_S OR (ca.minutes_on_support_S IS NULL AND noma.minutes_on_support_S IS NULL)) AND
						StudyPatientID=noma.StudyPatientID AND
						(ca.stay_count=noma.stay_count OR (ca.stay_count IS NULL AND noma.stay_count IS NULL)) AND
						(ca.beg_utc=noma.beg_utc OR (ca.beg_utc IS NULL AND noma.beg_utc IS NULL)) AND
						(ca.end_utc=noma.end_utc OR (ca.end_utc IS NULL AND noma.end_utc IS NULL)) AND
						(ca.earliest_support_utc=noma.earliest_support_utc OR (ca.earliest_support_utc IS NULL AND noma.earliest_support_utc IS NULL)) AND
						(ca.latest_suport_utc=noma.latest_suport_utc OR (ca.latest_suport_utc IS NULL AND noma.latest_suport_utc IS NULL)) AND
						(ca.includes_organSupport=noma.includes_organSupport OR (ca.includes_organSupport IS NULL AND noma.includes_organSupport IS NULL)) AND
						(ca.includes_stepdownUnit=noma.includes_stepdownUnit OR (ca.includes_stepdownUnit IS NULL AND noma.includes_stepdownUnit IS NULL)) AND
						(ca.includes_ignoreUnit=noma.includes_ignoreUnit OR (ca.includes_ignoreUnit IS NULL AND noma.includes_ignoreUnit IS NULL)) AND
						(ca.includes_EDUnit=noma.includes_EDUnit OR (ca.includes_EDUnit IS NULL AND noma.includes_EDUnit IS NULL)) AND
						(ca.beg_utc_is_organ_support_adjusted=noma.beg_utc_is_organ_support_adjusted OR (ca.beg_utc_is_organ_support_adjusted IS NULL AND noma.beg_utc_is_organ_support_adjusted IS NULL)) AND
						(ca.Vaso_min_utc=noma.Vaso_min_utc OR (ca.Vaso_min_utc IS NULL AND noma.Vaso_min_utc IS NULL)) AND
						(ca.Vaso_max_utc=noma.Vaso_max_utc OR (ca.Vaso_max_utc IS NULL AND noma.Vaso_max_utc IS NULL)) AND
						(ca.HFNC_min_utc=noma.HFNC_min_utc OR (ca.HFNC_min_utc IS NULL AND noma.HFNC_min_utc IS NULL)) AND
						(ca.HFNC_max_utc=noma.HFNC_max_utc OR (ca.HFNC_max_utc IS NULL AND noma.HFNC_max_utc IS NULL)) AND
						(ca.ECMO_min_utc=noma.ECMO_min_utc OR (ca.ECMO_min_utc IS NULL AND noma.ECMO_min_utc IS NULL)) AND
						(ca.ECMO_max_utc=noma.ECMO_max_utc OR (ca.ECMO_max_utc IS NULL AND noma.ECMO_max_utc IS NULL)) AND
						(ca.Niv_min_utc=noma.Niv_min_utc OR (ca.Niv_min_utc IS NULL AND noma.Niv_min_utc IS NULL)) AND
						(ca.Niv_max_utc=noma.Niv_max_utc OR (ca.Niv_max_utc IS NULL AND noma.Niv_max_utc IS NULL)) AND
						(ca.IMV_min_utc=noma.IMV_min_utc OR (ca.IMV_min_utc IS NULL AND noma.IMV_min_utc IS NULL)) AND
						(ca.IMV_max_utc=noma.IMV_max_utc OR (ca.IMV_max_utc IS NULL AND noma.IMV_max_utc IS NULL)) 
				);				
	

SELECT 'v3IcuDaysOnSupport' AS SOURCE_TABLE, 'NOMA' AS DATA_SOURCE, COUNT(*) AS COUNTS FROM REMAP.v3IcuDaysOnSupport
UNION
SELECT 'v3IcuDaysOnSupport', 'CA', COUNT(*) FROM REMAP_CA.v3IcuDaysOnSupport  
UNION
SELECT 'v3IcuDaysOnSupport', 'CA EXISTS NOMA', COUNT(*) 
FROM REMAP_CA.v3IcuDaysOnSupport ca 
WHERE EXISTS(	SELECT * 
					FROM REMAP.v3IcuDaysOnSupport noma 
					WHERE STUDYPATIENTID=noma.STUDYPATIENTID AND
						(ca.number_of_ICU_stays=noma.number_of_ICU_stays OR (ca.number_of_ICU_stays IS NULL AND noma.number_of_ICU_stays IS NULL)) AND
						(ca.first_ICU_admit=noma.first_ICU_admit OR (ca.first_ICU_admit IS NULL AND noma.first_ICU_admit IS NULL)) AND
						(ca.last_ICU_discharge=noma.last_ICU_discharge OR (ca.last_ICU_discharge IS NULL AND noma.last_ICU_discharge IS NULL)) AND
						(ca.first_support_utc=noma.first_support_utc OR (ca.first_support_utc IS NULL AND noma.first_support_utc IS NULL)) AND
						(ca.last_support_utc=noma.last_support_utc OR (ca.last_support_utc IS NULL AND noma.last_support_utc IS NULL)) AND
						(ca.hours_on_support_M=noma.hours_on_support_M OR (ca.hours_on_support_M IS NULL AND noma.hours_on_support_M IS NULL)) AND
						(ca.hours_on_support_S=noma.hours_on_support_S OR (ca.hours_on_support_S IS NULL AND noma.hours_on_support_S IS NULL)) 
				);			
	
					