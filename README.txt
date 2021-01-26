REMAP

Queries for automated eCRF of the REMAP-CAP trial. 

The main file is REMAP-v3-build.sql. It is supported by functions defined in REMAP-v3-definedFunctions.sql and views defined in REMAP-v3-definedViews.sql.

REMAP-v3-reportingForms.sql will consist of the correct formatting of sub-sections of the CRF forms 2. Baseline, 4. Daily, and 6. Discharge, as well as, the SAC RAR form. This is file is still a work in progress. 

The legacy v2 tables are populated from v3 tables using REMAP-v3-updateV2tables. Legacy v2 views are not yet updated to the v3 architecture. When they are, a file titled REMAP-v3-supportingV2views.sql will be added to this repository. 

To perform a table refreah of the Cerner data: 
	(1) run all of REMAP-v3-build.sql
	(2) run all of REMAP-v3-updateV2tables.sql
	
Pinnicle data is not yet suppoted. However, parial implementations are provided in REMAP-v3-buildEpic and REMAP-v3-updateV2tablesEpic (and, in the future, REMAP-v3-supportingV2viewsEpic.sql).  

