RAR-queries_scripts-2020Nov01

To refreash tables:

STEPS:
	Run query event-primer-2020Nov01.sql
	Run script v2KeeperFins (is reliant on v2EnrolledPersonPrimer and v2LastEvent)
	Run script v2locationInterperitation (is reliant on v2EnrolledPersonPrimer and v2LastEvent)
	Run query event-supportTables-DaysOnSupport-2020Nov01.sql
	Run script v2ScriptHypoxia
	Run query event-supportTables-Apachee-2020Nov01.sql
	fin