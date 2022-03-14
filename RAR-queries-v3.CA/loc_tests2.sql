SELECT * FROM REMAP.testOldv3IcuStay WHERE studypatientid = '0400100141';
SELECT * FROM REMAP.testNewv3IcuStay WHERE studypatientid = '0400100141';
SELECT * FROM REMAP.testRevizedv3IcuStay WHERE studypatientid = '0400100141';
SELECT * FROM REMAP.v3UnitStay WHERE studypatientid  = '0400100141';
SELECT studypatientid, MIN(event_utc) FROM REMAP.v3OrganSupportInstance WHERE studypatientid = '0400100141' 
GROUP BY studypatientid ORDER BY studypatientid;