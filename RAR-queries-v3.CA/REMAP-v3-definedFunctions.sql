
DROP FUNCTION REMAP.to_utc;

delimiter $$

CREATE FUNCTION REMAP.to_utc(date_local datetime) RETURNS datetime DETERMINISTIC
BEGIN
	RETURN if(date_local = '2100-12-31 00:00:00', '2100-12-31 00:00:00', CASE
	    WHEN date_local BETWEEN '2020-03-31 02:00:00' AND '2020-11-01 01:00:00' THEN CONVERT_TZ(date_local,'-04:00','+00:00')
	    WHEN date_local BETWEEN '2021-03-14 02:00:00' AND '2021-11-07 01:00:00' THEN CONVERT_TZ(date_local,'-04:00','+00:00')
  	    WHEN date_local BETWEEN '2022-03-13 02:00:00' AND '2022-11-06 01:00:00' THEN CONVERT_TZ(date_local,'-04:00','+00:00')
	    ELSE CONVERT_TZ(date_local,'-05:00','+00:00')
  END);
END$$

delimiter ;



DROP FUNCTION REMAP.to_local;

delimiter $$

CREATE FUNCTION REMAP.to_local(date_utc datetime) RETURNS datetime DETERMINISTIC
BEGIN
	RETURN if(date_utc = '2100-12-31 00:00:00', '2100-12-31 00:00:00', CASE
	    WHEN date_utc BETWEEN '2020-03-31 02:00:00' AND '2020-11-01 01:00:00' THEN CONVERT_TZ(date_utc,'+04:00','+00:00')
	    WHEN date_utc BETWEEN '2021-03-14 02:00:00' AND '2021-11-07 01:00:00' THEN CONVERT_TZ(date_utc,'+04:00','+00:00')
  	    WHEN date_utc BETWEEN '2022-03-13 02:00:00' AND '2022-11-06 01:00:00' THEN CONVERT_TZ(date_utc,'+04:00','+00:00')
	    ELSE CONVERT_TZ(date_utc,'+05:00','+00:00')
  END);
END$$

delimiter ;


# for the study day function
DROP FUNCTION REMAP.to_datetime_utc;

delimiter $$

CREATE FUNCTION REMAP.to_datetime_utc(date_local DATE) RETURNS datetime DETERMINISTIC
BEGIN
	DECLARE datetime_local DATETIME;
	SET datetime_local = CONVERT(date_local, DATETIME);
	RETURN REMAP.to_utc(datetime_local);
END$$

delimiter ;


DROP FUNCTION REMAP.to_datetime_epic;

delimiter $$

CREATE FUNCTION REMAP.to_datetime_epic(date_epic TEXT) RETURNS datetime DETERMINISTIC
BEGIN
	RETURN STR_TO_DATE(REPLACE(date_epic, '.000000000', ''), '%d-%b-%y %h.%i.%s %p');
END$$

delimiter ;


# for geting unique keys
DROP FUNCTION REMAP.to_unique_orderd_num;

delimiter $$

CREATE FUNCTION REMAP.to_unique_orderd_num(row_date DATETIME, ptid VARCHAR(100)) RETURNS BIGINT DETERMINISTIC
BEGIN
	RETURN UNIX_TIMESTAMP(row_date)*10000+SUBSTRING(ptid, -4);
END$$

delimiter ;


# for null dates, convert to default value
DROP FUNCTION REMAP.dfltH;

delimiter $$

CREATE FUNCTION REMAP.dfltH(date_in DATETIME) RETURNS datetime DETERMINISTIC
BEGIN
	DECLARE datetime_out DATETIME;
	SET datetime_out = IFNULL(date_in, '2030-01-01 00:00:00');
	RETURN datetime_out;
END$$

delimiter ;

# for null dates, convert to default value
DROP FUNCTION REMAP.dfltL;

delimiter $$

CREATE FUNCTION REMAP.dfltL(date_in DATETIME) RETURNS datetime DETERMINISTIC
BEGIN
	DECLARE datetime_out DATETIME;
	SET datetime_out = IFNULL(date_in, '2010-01-01 00:00:00');
	RETURN datetime_out;
END$$

delimiter ;



/* ******************************************************************************************** */
/* SOURCE: https://stackoverflow.com/questions/37268248/how-to-get-only-digits-from-string-in-mysql */

DROP FUNCTION REMAP.to_float;

DELIMITER $$

CREATE FUNCTION REMAP.to_float(in_string VARCHAR(255)) 
RETURNS decimal(15,3)
NO SQL
BEGIN
    DECLARE ctrNumber VARCHAR(255);
    DECLARE in_string_parsed VARCHAR(255);
    DECLARE digitsAndDotsNumber VARCHAR(255) DEFAULT '';
    DECLARE finalNumber VARCHAR(255) DEFAULT '';
    DECLARE sChar VARCHAR(1);
    DECLARE inti INTEGER DEFAULT 1;
    DECLARE digitSequenceStarted boolean DEFAULT false;
    DECLARE negativeNumber boolean DEFAULT false;
    DECLARE decimalLocation INTEGER DEFAULT 0;
    DECLARE isNegStr boolean DEFAULT false;

    SET in_string_parsed = in_string;    

	 SET isNegStr = if(LOCATE('Neg', in_string_parsed)>0, true, false);
	
	 IF isNegStr THEN
		
		RETURN CAST(0.0 AS decimal(15,4));
		
    ELSEIF LENGTH(in_string_parsed) > 0 THEN
        # extract digits and dots
        WHILE(inti <= LENGTH(in_string_parsed)) DO
            SET sChar = SUBSTRING(in_string_parsed, inti, 1);
            SET ctrNumber = FIND_IN_SET(sChar, '0,1,2,3,4,5,6,7,8,9,.'); 
            IF ctrNumber > 0 AND (sChar != '.' OR LENGTH(digitsAndDotsNumber) > 0) THEN
                # add first minus if needed
                IF digitSequenceStarted = false AND inti > 1 AND SUBSTRING(in_string_parsed, inti-1, 1) = '-' THEN
                    SET negativeNumber = true;
                END IF;

                SET digitSequenceStarted = true;
                SET digitsAndDotsNumber = CONCAT(digitsAndDotsNumber, sChar);
            ELSEIF digitSequenceStarted = true THEN
                SET inti = LENGTH(in_string_parsed);
            END IF;
            SET inti = inti + 1;
        END WHILE;

        # remove dots from the end of number list
        SET inti = LENGTH(digitsAndDotsNumber);
        WHILE(inti > 0) DO
            IF(SUBSTRING(digitsAndDotsNumber, inti, 1) = '.') THEN
                SET digitsAndDotsNumber = SUBSTRING(digitsAndDotsNumber, 1, inti-1);
                SET inti = inti - 1;
            ELSE
                SET inti = 0;
            END IF;
        END WHILE;

		  SET finalNumber = digitsAndDotsNumber;

        IF negativeNumber = true AND LENGTH(finalNumber) > 0 THEN
            SET finalNumber = CONCAT('-', finalNumber);
        END IF;

        IF LENGTH(finalNumber) = 0 THEN
            RETURN NULL;
        END IF;

        RETURN CAST(finalNumber AS decimal(15,4));
    ELSE
        RETURN 0;
    END IF;    
END$$

DELIMITER ;





/* ******************************************************************************************** */

DROP FUNCTION REMAP.convert_vaso_to_sofa_points;

DELIMITER $$

CREATE FUNCTION REMAP.convert_vaso_to_sofa_points(med_name VARCHAR(255), converted_dose FLOAT) 
RETURNS INTEGER
NO SQL
BEGIN
   DECLARE score INTEGER DEFAULT 0; 
	
	 	IF med_name = 'Dobutamine' THEN
		 	SET score = 2;
		ELSEIF med_name = 'Dopamine' THEN 
			IF converted_dose <= 5 THEN 
				SET score = 2;
			ELSEIF converted_dose <= 15 THEN 
				SET score = 3;
			ELSEIF converted_dose > 15 THEN 
				SET score = 4;
			ELSE 
				SET score = 1;
			END IF;
		ELSEIF med_name = 'Epinephrine' THEN 
			IF converted_dose <= 0.1 THEN 
				SET score = 3;
			ELSEIF converted_dose <= 0.3 THEN 
				SET score = 4;
			ELSEIF converted_dose > 0.3 THEN 
				SET score = 5;
			ELSE 
				SET score = 1;
			END IF;
		ELSEIF med_name IN ('Norepinephrine', 'norepinephrine') THEN 
			IF converted_dose <= 0.1 THEN 
				SET score = 3;
			ELSEIF converted_dose <= 0.3 THEN 
				SET score = 4;
			ELSEIF converted_dose > 0.3 THEN 
				SET score = 5;
			ELSE 
				SET score = 1;
			END IF;
		ELSEIF med_name IN ('Phenylephrine', 'phenylephrine 1') THEN 
			SET score = 3;
		ELSEIF med_name IN ('Vasopressin', 'vasopressin') THEN 
			SET score = 3;
		ELSEIF med_name = 'Milrinone' THEN 
			SET score = 2;
		ELSEIF med_name = 'Metaraminol' THEN 
			SET score = 3;
		ELSE 
			SET score = 1;
		END IF;

   RETURN score;

END$$

DELIMITER ;


/* ******************************************************************************************** */
DROP FUNCTION REMAP.get_prefix;

DELIMITER $$

CREATE FUNCTION REMAP.get_prefix(in_string VARCHAR(255)) 
RETURNS VARCHAR(3)
NO SQL
BEGIN
    DECLARE preFix VARCHAR(3) DEFAULT '';
    DECLARE in_string_parsed VARCHAR(255);

    SET in_string_parsed = in_string;    

	IF in_string_parsed REGEXP '^[0-9]' THEN
		 SET preFix = '';
	ELSEIF LOCATE('Neg', in_string_parsed)>0 THEN		
		SET preFix = 'NEG';
    ELSEIF LOCATE('<=', in_string_parsed)>0 THEN
		SET preFix = '<=';    
    ELSEIF LOCATE('>=', in_string_parsed)>0 THEN
		SET preFix = '>=';    
    ELSEIF LOCATE('>', in_string_parsed)>0 THEN
		SET preFix = '>'; 
    ELSEIF LOCATE('<', in_string_parsed)>0 THEN
		SET preFix = '<';       
    ELSEIF LOCATE('+', in_string_parsed)>0 THEN
		SET preFix = '+';   
		
    END IF;    
    
    RETURN preFix;

END$$

DELIMITER ;
/* ******************************************************************************************** */

DROP FUNCTION REMAP.extract_diastolic;

DELIMITER $$

CREATE FUNCTION REMAP.extract_diastolic(in_string VARCHAR(100)) 
RETURNS VARCHAR(100)
NO SQL
BEGIN
    DECLARE diastolic VARCHAR(100) DEFAULT '';

	 SET diastolic = SUBSTRING(in_string,  POSITION('/' IN in_string)+1);

    RETURN diastolic;

END$$

DELIMITER ;

/* ******************************** */
/* baseline standard (this should be moved to standardization table by requires chris' input */ 

DROP FUNCTION REMAP.to_baseline_standard;

DELIMITER $$

CREATE FUNCTION REMAP.to_baseline_standard(in_string VARCHAR(100)) 
RETURNS VARCHAR(100)
NO SQL
BEGIN
    DECLARE baseline_standard VARCHAR(100) DEFAULT ''; 

	IF in_string IN ('Creatinine','Cr') THEN
		 SET baseline_standard = 'Cr';
	ELSEIF in_string IN ('Creatinine (iStat)', 'Creatinine (whole blood)') THEN		
		SET baseline_standard = 'Cr_aux';
    ELSEIF in_string IN ('Platelet count','Platelet count (DIC screen)','Platelets','Platelet count (PFA)') THEN
		SET baseline_standard = 'Plt';    
    ELSEIF in_string IN ('Bilirubin total') THEN
		SET baseline_standard = 'TBili';    
    ELSEIF in_string IN ('Bilirubin total (whole blood)') THEN
		SET baseline_standard = 'TBili_aux'; 
    ELSEIF in_string IN ('Lactate', 'Lactic Acid', 'Lactic Acid (arterial)', 'Lactic Acid (venous)', 'Lactate (no data)',  'Lactate (venous)') THEN
		SET baseline_standard = 'Lactate';       
    ELSEIF in_string IN ('Lactate (whole blood)','Lactate (arterial iStat)','Lactate (venous iStat)', 'Lactate (arterial iStat)' ,'Lactic Acid (iStat)', 'Lactate (iStat)','Lactate (arterial respiratory)') THEN
		SET baseline_standard = 'Lactate_aux';   
	ELSEIF in_string IN ('Glasgow Coma Score (total)') THEN		
		SET baseline_standard = 'GCS';
	ELSEIF in_string IN ('Troponin T','Troponin I (iStat)','Troponin I','Troponin (unknown)','Troponin (comment)') THEN		
		SET baseline_standard = 'Troponin';
	ELSEIF in_string IN ('INR','INR (comment)') THEN		
		SET baseline_standard = 'INR';
	ELSEIF in_string IN ('Temperature (conversion)', 'Temperature (metric)', 'Temperature') THEN		
		SET baseline_standard = 'Temp';
	ELSEIF in_string IN ('Blood pressure (arterial systolic)' ) THEN		
		SET baseline_standard = 'BP_sys';
	ELSEIF in_string IN ('Blood pressure (systolic)' ) THEN		
		SET baseline_standard = 'BP_sys_aux';
	ELSEIF in_string IN ('Bicarbonate','Bicarbonate (iStat)') THEN		
		SET baseline_standard = 'HCO3';
	ELSE 
		SET baseline_standard = in_string;
   END IF;    
    
    RETURN baseline_standard;

END$$

DELIMITER ;

/* ******************************** */
/* contains all the distinct units from lab table */ 
DROP FUNCTION REMAP.get_postfix;

DELIMITER $$

CREATE FUNCTION REMAP.get_postfix(in_string VARCHAR(255)) 
RETURNS VARCHAR(16)
NO SQL

BEGIN
    DECLARE postFix VARCHAR(16) DEFAULT '';
    DECLARE in_string_parsed VARCHAR(255);

    SET in_string_parsed = in_string;    

	# lab units
	IF in_string_parsed REGEXP '[0-9]$' THEN
		 SET postfix = '';
	ELSEIF LOCATE('%', in_string_parsed)>0 THEN
		 SET postfix = '%';
	 ELSEIF LOCATE('% (g/dL)', in_string_parsed)>0 THEN
		 SET postfix = '% (g/dL)';
	 ELSEIF LOCATE('% Bound', in_string_parsed)>0 THEN
		 SET postfix = '% Bound';
	 ELSEIF LOCATE('% Free', in_string_parsed)>0 THEN
		 SET postfix = '% Free';
	 ELSEIF LOCATE('% of total Hgb', in_string_parsed)>0 THEN
		 SET postfix = '% of total Hgb';
	 ELSEIF LOCATE('(calc)', in_string_parsed)>0 THEN
		 SET postfix = '(calc)';
	 ELSEIF LOCATE('/100 WBC', in_string_parsed)>0 THEN
		 SET postfix = '/100 WBC';
	 ELSEIF LOCATE('/cmm', in_string_parsed)>0 THEN
		 SET postfix = '/cmm';
	 ELSEIF LOCATE('/CMM', in_string_parsed)>0 THEN
		 SET postfix = '/CMM';
	 ELSEIF LOCATE('/cu mm', in_string_parsed)>0 THEN
		 SET postfix = '/cu mm';
	 ELSEIF LOCATE('/hpf', in_string_parsed)>0 THEN
		 SET postfix = '/hpf';
	 ELSEIF LOCATE('/lfp', in_string_parsed)>0 THEN
		 SET postfix = '/lfp';
	 ELSEIF LOCATE('/lpf', in_string_parsed)>0 THEN
		 SET postfix = '/lpf';
	 ELSEIF LOCATE('/mL', in_string_parsed)>0 THEN
		 SET postfix = '/mL';
	 ELSEIF LOCATE('/mm', in_string_parsed)>0 THEN
		 SET postfix = '/mm';
	 ELSEIF LOCATE('/mm3', in_string_parsed)>0 THEN
		 SET postfix = '/mm3';
	 ELSEIF LOCATE('/mmE+03', in_string_parsed)>0 THEN
		 SET postfix = '/mmE+03';
	 ELSEIF LOCATE('/uL', in_string_parsed)>0 THEN
		 SET postfix = '/uL';
	 ELSEIF LOCATE('10E+12/L', in_string_parsed)>0 THEN
		 SET postfix = '10E+12/L';
	 ELSEIF LOCATE('10E+9/L', in_string_parsed)>0 THEN
		 SET postfix = '10E+9/L';
	 ELSEIF LOCATE('Absolute', in_string_parsed)>0 THEN
		 SET postfix = 'Absolute';
	 ELSEIF LOCATE('AI', in_string_parsed)>0 THEN
		 SET postfix = 'AI';
	 ELSEIF LOCATE('ARU', in_string_parsed)>0 THEN
		 SET postfix = 'ARU';
	 ELSEIF LOCATE('b/min', in_string_parsed)>0 THEN
		 SET postfix = 'b/min';
	 ELSEIF LOCATE('BPM', in_string_parsed)>0 THEN
		 SET postfix = 'BPM';
	 ELSEIF LOCATE('cc', in_string_parsed)>0 THEN
		 SET postfix = 'cc';
	 ELSEIF LOCATE('CELCIUS', in_string_parsed)>0 THEN
		 SET postfix = 'CELCIUS';
	 ELSEIF LOCATE('Cells', in_string_parsed)>0 THEN
		 SET postfix = 'Cells';
	 ELSEIF LOCATE('cells/uL', in_string_parsed)>0 THEN
		 SET postfix = 'cells/uL';
	 ELSEIF LOCATE('cmH20', in_string_parsed)>0 THEN
		 SET postfix = 'cmH20';
	 ELSEIF LOCATE('copies/ml', in_string_parsed)>0 THEN
		 SET postfix = 'copies/ml';
	 ELSEIF LOCATE('COUNTED', in_string_parsed)>0 THEN
		 SET postfix = 'COUNTED';
	 ELSEIF LOCATE('cu.mm', in_string_parsed)>0 THEN
		 SET postfix = 'cu.mm';
	 ELSEIF LOCATE('CUMM', in_string_parsed)>0 THEN
		 SET postfix = 'CUMM';
	 ELSEIF LOCATE('d/sc', in_string_parsed)>0 THEN
		 SET postfix = 'd/sc';
	 ELSEIF LOCATE('Degrees', in_string_parsed)>0 THEN
		 SET postfix = 'Degrees';
	 ELSEIF LOCATE('dils', in_string_parsed)>0 THEN
		 SET postfix = 'dils';
	 ELSEIF LOCATE('Ehrlich U/dL', in_string_parsed)>0 THEN
		 SET postfix = 'Ehrlich U/dL';
	 ELSEIF LOCATE('EoC', in_string_parsed)>0 THEN
		 SET postfix = 'EoC';
	 ELSEIF LOCATE('eos/oif', in_string_parsed)>0 THEN
		 SET postfix = 'eos/oif';
	 ELSEIF LOCATE('EU', in_string_parsed)>0 THEN
		 SET postfix = 'EU';
	 ELSEIF LOCATE('EU/dL', in_string_parsed)>0 THEN
		 SET postfix = 'EU/dL';
	 ELSEIF LOCATE('fL', in_string_parsed)>0 THEN
		 SET postfix = 'fL';
	 ELSEIF LOCATE('G%', in_string_parsed)>0 THEN
		 SET postfix = 'G%';
	 ELSEIF LOCATE('g/24H', in_string_parsed)>0 THEN
		 SET postfix = 'g/24H';
	 ELSEIF LOCATE('g/dl', in_string_parsed)>0 THEN
		 SET postfix = 'g/dl';
	 ELSEIF LOCATE('g/Dl', in_string_parsed)>0 THEN
		 SET postfix = 'g/Dl';
	 ELSEIF LOCATE('g/dL (calc)', in_string_parsed)>0 THEN
		 SET postfix = 'g/dL (calc)';
	 ELSEIF LOCATE('g/dl.', in_string_parsed)>0 THEN
		 SET postfix = 'g/dl.';
	 ELSEIF LOCATE('g/L', in_string_parsed)>0 THEN
		 SET postfix = 'g/L';
	 ELSEIF LOCATE('gm(percent)', in_string_parsed)>0 THEN
		 SET postfix = 'gm(percent)';
	 ELSEIF LOCATE('gm/24h', in_string_parsed)>0 THEN
		 SET postfix = 'gm/24h';
	 ELSEIF LOCATE('gm/24hr', in_string_parsed)>0 THEN
		 SET postfix = 'gm/24hr';
	 ELSEIF LOCATE('gm/dL', in_string_parsed)>0 THEN
		 SET postfix = 'gm/dL';
	 ELSEIF LOCATE('HR', in_string_parsed)>0 THEN
		 SET postfix = 'HR';
	 ELSEIF LOCATE('HR:MIN', in_string_parsed)>0 THEN
		 SET postfix = 'HR:MIN';
	 ELSEIF LOCATE('hr:min:sec', in_string_parsed)>0 THEN
		 SET postfix = 'hr:min:sec';
	 ELSEIF LOCATE('ISR', in_string_parsed)>0 THEN
		 SET postfix = 'ISR';
	 ELSEIF LOCATE('IU/L', in_string_parsed)>0 THEN
		 SET postfix = 'IU/L';
	 ELSEIF LOCATE('IU/mL', in_string_parsed)>0 THEN
		 SET postfix = 'IU/mL';
	 ELSEIF LOCATE('k/cmm', in_string_parsed)>0 THEN
		 SET postfix = 'k/cmm';
	 ELSEIF LOCATE('K/uL', in_string_parsed)>0 THEN
		 SET postfix = 'K/uL';
	 ELSEIF LOCATE('KU/L', in_string_parsed)>0 THEN
		 SET postfix = 'KU/L';
	 ELSEIF LOCATE('L/min', in_string_parsed)>0 THEN
		 SET postfix = 'L/min';
	 ELSEIF LOCATE('Leu/uL', in_string_parsed)>0 THEN
		 SET postfix = 'Leu/uL';
	 ELSEIF LOCATE('LOGIU/ML', in_string_parsed)>0 THEN
		 SET postfix = 'LOGIU/ML';
	 ELSEIF LOCATE('M', in_string_parsed)>0 THEN
		 SET postfix = 'M';
	 ELSEIF LOCATE('m/cmm', in_string_parsed)>0 THEN
		 SET postfix = 'm/cmm';
	 ELSEIF LOCATE('M/mL', in_string_parsed)>0 THEN
		 SET postfix = 'M/mL';
	 ELSEIF LOCATE('M/uL', in_string_parsed)>0 THEN
		 SET postfix = 'M/uL';
	 ELSEIF LOCATE('mcg/mL', in_string_parsed)>0 THEN
		 SET postfix = 'mcg/mL';
	 ELSEIF LOCATE('mEq/L', in_string_parsed)>0 THEN
		 SET postfix = 'mEq/L';
	 ELSEIF LOCATE('mg Alb/g Creat', in_string_parsed)>0 THEN
		 SET postfix = 'mg Alb/g Creat';
	 ELSEIF LOCATE('mg/24 hr', in_string_parsed)>0 THEN
		 SET postfix = 'mg/24 hr';
	 ELSEIF LOCATE('mg/24H', in_string_parsed)>0 THEN
		 SET postfix = 'mg/24H';
	 ELSEIF LOCATE('mg/dL', in_string_parsed)>0 THEN
		 SET postfix = 'mg/dL';
	 ELSEIF LOCATE('mg/dL (calc)', in_string_parsed)>0 THEN
		 SET postfix = 'mg/dL (calc)';
	 ELSEIF LOCATE('mg/G', in_string_parsed)>0 THEN
		 SET postfix = 'mg/G';
	 ELSEIF LOCATE('mg/gm', in_string_parsed)>0 THEN
		 SET postfix = 'mg/gm';
	 ELSEIF LOCATE('mg/gm Creat', in_string_parsed)>0 THEN
		 SET postfix = 'mg/gm Creat';
	 ELSEIF LOCATE('mg/L', in_string_parsed)>0 THEN
		 SET postfix = 'mg/L';
	 ELSEIF LOCATE('mg/L FEU', in_string_parsed)>0 THEN
		 SET postfix = 'mg/L FEU';
	 ELSEIF LOCATE('mg/TVol', in_string_parsed)>0 THEN
		 SET postfix = 'mg/TVol';
	 ELSEIF LOCATE('mil/mm3', in_string_parsed)>0 THEN
		 SET postfix = 'mil/mm3';
	 ELSEIF LOCATE('Mill/cu.mm', in_string_parsed)>0 THEN
		 SET postfix = 'Mill/cu.mm';
	 ELSEIF LOCATE('Million/uL', in_string_parsed)>0 THEN
		 SET postfix = 'Million/uL';
	 ELSEIF LOCATE('min', in_string_parsed)>0 THEN
		 SET postfix = 'min';
	 ELSEIF LOCATE('minutes', in_string_parsed)>0 THEN
		 SET postfix = 'minutes';
	 ELSEIF LOCATE('mIU/L', in_string_parsed)>0 THEN
		 SET postfix = 'mIU/L';
	 ELSEIF LOCATE('mIU/mL', in_string_parsed)>0 THEN
		 SET postfix = 'mIU/mL';
	 ELSEIF LOCATE('mL', in_string_parsed)>0 THEN
		 SET postfix = 'mL';
	 ELSEIF LOCATE('mL/dL', in_string_parsed)>0 THEN
		 SET postfix = 'mL/dL';
	 ELSEIF LOCATE('mL/min', in_string_parsed)>0 THEN
		 SET postfix = 'mL/min';
	 ELSEIF LOCATE('mL/min/1.73m2', in_string_parsed)>0 THEN
		 SET postfix = 'mL/min/1.73m2';
	 ELSEIF LOCATE('mm', in_string_parsed)>0 THEN
		 SET postfix = 'mm';
	 ELSEIF LOCATE('mm/hr', in_string_parsed)>0 THEN
		 SET postfix = 'mm/hr';
	 ELSEIF LOCATE('MM/hr', in_string_parsed)>0 THEN
		 SET postfix = 'MM/hr';
	 ELSEIF LOCATE('mmE+03', in_string_parsed)>0 THEN
		 SET postfix = 'mmE+03';
	 ELSEIF LOCATE('mmHg', in_string_parsed)>0 THEN
		 SET postfix = 'mmHg';
	 ELSEIF LOCATE('mMol/L', in_string_parsed)>0 THEN
		 SET postfix = 'mMol/L';
	 ELSEIF LOCATE('mOs/kg', in_string_parsed)>0 THEN
		 SET postfix = 'mOs/kg';
	 ELSEIF LOCATE('mOsm', in_string_parsed)>0 THEN
		 SET postfix = 'mOsm';
	 ELSEIF LOCATE('mOsm/kg', in_string_parsed)>0 THEN
		 SET postfix = 'mOsm/kg';
	 ELSEIF LOCATE('N', in_string_parsed)>0 THEN
		 SET postfix = 'N';
	 ELSEIF LOCATE('ng/dL', in_string_parsed)>0 THEN
		 SET postfix = 'ng/dL';
	 ELSEIF LOCATE('ng/mL', in_string_parsed)>0 THEN
		 SET postfix = 'ng/mL';
	 ELSEIF LOCATE('ng/ml', in_string_parsed)>0 THEN
		 SET postfix = 'ng/ml';
	 ELSEIF LOCATE('pg', in_string_parsed)>0 THEN
		 SET postfix = 'pg';
	 ELSEIF LOCATE('pg/dL', in_string_parsed)>0 THEN
		 SET postfix = 'pg/dL';
	 ELSEIF LOCATE('pg/mL', in_string_parsed)>0 THEN
		 SET postfix = 'pg/mL';
	 ELSEIF LOCATE('polys/hpf', in_string_parsed)>0 THEN
		 SET postfix = 'polys/hpf';
	 ELSEIF LOCATE('PRU', in_string_parsed)>0 THEN
		 SET postfix = 'PRU';
	 ELSEIF LOCATE('RATE', in_string_parsed)>0 THEN
		 SET postfix = 'RATE';
	 ELSEIF LOCATE('Ratio', in_string_parsed)>0 THEN
		 SET postfix = 'Ratio';
	 ELSEIF LOCATE('S/Co', in_string_parsed)>0 THEN
		 SET postfix = 'S/Co';
	 ELSEIF LOCATE('sec', in_string_parsed)>0 THEN
		 SET postfix = 'sec';
	 ELSEIF LOCATE('TH/mm3', in_string_parsed)>0 THEN
		 SET postfix = 'TH/mm3';
	 ELSEIF LOCATE('Therapeutic', in_string_parsed)>0 THEN
		 SET postfix = 'Therapeutic';
	 ELSEIF LOCATE('Thou/uL', in_string_parsed)>0 THEN
		 SET postfix = 'Thou/uL';
	 ELSEIF LOCATE('Thous/cu.mm', in_string_parsed)>0 THEN
		 SET postfix = 'Thous/cu.mm';
	 ELSEIF LOCATE('Thousand/uL', in_string_parsed)>0 THEN
		 SET postfix = 'Thousand/uL';
	 ELSEIF LOCATE('U/mL', in_string_parsed)>0 THEN
		 SET postfix = 'U/mL';
	 ELSEIF LOCATE('ug/dL', in_string_parsed)>0 THEN
		 SET postfix = 'ug/dL';
	 ELSEIF LOCATE('ug/dl', in_string_parsed)>0 THEN
		 SET postfix = 'ug/dl';
	 ELSEIF LOCATE('ug/L', in_string_parsed)>0 THEN
		 SET postfix = 'ug/L';
	 ELSEIF LOCATE('ug/mL', in_string_parsed)>0 THEN
		 SET postfix = 'ug/mL';
	 ELSEIF LOCATE('ug/mL FEU', in_string_parsed)>0 THEN
		 SET postfix = 'ug/mL FEU';
	 ELSEIF LOCATE('ug/ml.', in_string_parsed)>0 THEN
		 SET postfix = 'ug/ml.';
	 ELSEIF LOCATE('UI/mL', in_string_parsed)>0 THEN
		 SET postfix = 'UI/mL';
	 ELSEIF LOCATE('uIU/mL', in_string_parsed)>0 THEN
		 SET postfix = 'uIU/mL';
	 ELSEIF LOCATE('uL', in_string_parsed)>0 THEN
		 SET postfix = 'uL';
	 ELSEIF LOCATE('uM/L', in_string_parsed)>0 THEN
		 SET postfix = 'uM/L';
	 ELSEIF LOCATE('um3', in_string_parsed)>0 THEN
		 SET postfix = 'um3';
	 ELSEIF LOCATE('uMe3', in_string_parsed)>0 THEN
		 SET postfix = 'uMe3';
	 ELSEIF LOCATE('uMol/L', in_string_parsed)>0 THEN
		 SET postfix = 'uMol/L';
	 ELSEIF LOCATE('Unit(s)/L', in_string_parsed)>0 THEN
		 SET postfix = 'Unit(s)/L';
	 ELSEIF LOCATE('uUg', in_string_parsed)>0 THEN
		 SET postfix = 'uUg';
	 ELSEIF LOCATE('Vol %', in_string_parsed)>0 THEN
		 SET postfix = 'Vol %';
	 ELSEIF LOCATE('x10', in_string_parsed)>0 THEN
		 SET postfix = 'x10';
	 ELSEIF LOCATE('X10e+03/uL', in_string_parsed)>0 THEN
		 SET postfix = 'X10e+03/uL';
	 ELSEIF LOCATE('X10e+06/uL', in_string_parsed)>0 THEN
		 SET postfix = 'X10e+06/uL';
	 ELSEIF LOCATE('X10E+09/L', in_string_parsed)>0 THEN
		 SET postfix = 'X10E+09/L';
	 ELSEIF LOCATE('X10E+12/L', in_string_parsed)>0 THEN
		 SET postfix = 'X10E+12/L';
	 ELSEIF LOCATE('X10E3/cumm', in_string_parsed)>0 THEN
		 SET postfix = 'X10E3/cumm';
	 ELSEIF LOCATE('X10e3/uL', in_string_parsed)>0 THEN
		 SET postfix = 'X10e3/uL';
	 ELSEIF LOCATE('x10e6/uL', in_string_parsed)>0 THEN
		 SET postfix = 'x10e6/uL';
	 ELSEIF LOCATE('X1OE+09/L', in_string_parsed)>0 THEN
		 SET postfix = 'X1OE+09/L';
	 ELSEIF LOCATE('xmmol/24h', in_string_parsed)>0 THEN
		 SET postfix = 'xmmol/24h';
	 ELSEIF LOCATE('xxnmol/L', in_string_parsed)>0 THEN
		 SET postfix = 'xxnmol/L';
	# physio units
	 ELSEIF LOCATE('cm', in_string_parsed)>0 THEN
		 SET postfix = 'cm';
	 ELSEIF LOCATE('kg', in_string_parsed)>0 THEN
		 SET postfix = 'kg';
	 ELSEIF LOCATE('lb', in_string_parsed)>0 THEN
		 SET postfix = 'lb';
	 ELSEIF LOCATE('DegF', in_string_parsed)>0 THEN
		 SET postfix = 'DegF';
	 ELSEIF LOCATE('L', in_string_parsed)>0 THEN
		 SET postfix = 'L';
	 ELSEIF LOCATE('mcg', in_string_parsed)>0 THEN
		 SET postfix = 'mcg';
	 ELSEIF LOCATE('ms', in_string_parsed)>0 THEN
		 SET postfix = 'ms';
	 ELSEIF LOCATE('mL/kg', in_string_parsed)>0 THEN
		 SET postfix = 'mL/kg';
	 ELSEIF LOCATE('mL/hr', in_string_parsed)>0 THEN
		 SET postfix = 'mL/hr';
	 ELSEIF LOCATE('RPM', in_string_parsed)>0 THEN
		 SET postfix = 'RPM';
	 ELSEIF LOCATE('mg', in_string_parsed)>0 THEN
		 SET postfix = 'mg';
	 ELSEIF LOCATE('ppm', in_string_parsed)>0 THEN
		 SET postfix = 'ppm';
	 ELSEIF LOCATE('Unit(s)/hr', in_string_parsed)>0 THEN
		 SET postfix = 'Unit(s)/hr';
	 ELSEIF LOCATE('br/min', in_string_parsed)>0 THEN
		 SET postfix = 'br/min';
	 ELSEIF LOCATE('Lpm', in_string_parsed)>0 THEN
		 SET postfix = 'Lpm';
	 ELSEIF LOCATE('PSI', in_string_parsed)>0 THEN
		 SET postfix = 'PSI';
	 ELSEIF LOCATE('mL/cmH20', in_string_parsed)>0 THEN
		 SET postfix = 'mL/cmH20';
	 ELSEIF LOCATE('in', in_string_parsed)>0 THEN
		 SET postfix = 'in';
	 ELSEIF LOCATE('DegC', in_string_parsed)>0 THEN
		 SET postfix = 'DegC';
	 ELSEIF LOCATE('bmp', in_string_parsed)>0 THEN
		 SET postfix = 'bmp';
    END IF;    
    
    RETURN postfix;

END$$

DELIMITER ; 


/* ****************************************************** */
	
DROP FUNCTION REMAP.get_physio_result_str;

DELIMITER $$

CREATE FUNCTION REMAP.get_physio_result_str(in_type VARCHAR(32), in_string VARCHAR(255)) 
RETURNS VARCHAR(16)
NO SQL
BEGIN
    DECLARE result_str VARCHAR(16) DEFAULT '';
    DECLARE in_string_parsed VARCHAR(255);

    SET in_string_parsed = in_string;    

	 IF in_type = 'Oxygen therapy delivery device' THEN
		 IF in_string_parsed LIKE '%high flow nasal cannula%' OR in_string_parsed LIKE '%hfnc%'OR in_string_parsed LIKE '%airvo%' OR in_string_parsed LIKE '%optiflo%'>0 THEN		
			SET result_str = 'HFNC device';	
		 ELSEIF in_string_parsed IN ('CPAP', 'Bipap') THEN 
			SET result_str = 'NIV device';	
	    ELSEIF in_string_parsed LIKE '%Endotracheal Tube%' OR (in_string_parsed LIKE '%Tracheostomy%' AND in_string_parsed LIKE '%Ventilator%') THEN
			SET result_str = 'IV device';	
		 ELSEIF in_string_parsed IN (SELECT source_text FROM COVID_SUPPLEMENT.TEXT_STANDARDIZATION WHERE source_table = 'CE_PHYSIO' AND sub_standard_meaning IN ('NC')) THEN
		  	SET result_str = 'NC';
		 ELSEIF in_string_parsed IN (SELECT source_text FROM COVID_SUPPLEMENT.TEXT_STANDARDIZATION WHERE source_table = 'CE_PHYSIO' AND sub_standard_meaning IN ('Mask')) THEN
		  	SET result_str = 'Mask';
		 ELSEIF in_string_parsed IN (SELECT source_text FROM COVID_SUPPLEMENT.TEXT_STANDARDIZATION WHERE source_table = 'CE_PHYSIO' AND sub_standard_meaning IN ('Nonrebreather')) THEN
		  	SET result_str = 'Nonrebreather';
		 ELSEIF in_string_parsed IN (SELECT source_text FROM COVID_SUPPLEMENT.TEXT_STANDARDIZATION WHERE source_table = 'CE_PHYSIO' AND sub_standard_meaning IN ('Prebreather')) THEN
		  	SET result_str = 'Prebreather';
		 END IF; 
	 ELSEIF in_type = 'ECMO' THEN
			SET result_str = in_string_parsed;
    ELSEIF in_type = 'Mode' THEN
		 IF in_string_parsed IN (SELECT source_text FROM COVID_SUPPLEMENT.TEXT_STANDARDIZATION WHERE sub_standard_meaning IN ('Noninvasive Modes')) THEN
		 	SET result_str = 'NIV mode';
	    ELSEIF in_string_parsed IN (SELECT source_text FROM COVID_SUPPLEMENT.TEXT_STANDARDIZATION WHERE sub_standard_meaning IN ('Invasive Modes')) THEN
			SET result_str = 'IV mode';
		 END IF;
	 ELSEIF in_type = 'Endotube placement' THEN
		 IF in_string_parsed IN (SELECT source_text FROM COVID_SUPPLEMENT.TEXT_STANDARDIZATION WHERE sub_standard_meaning IN ('Endotube positions')) THEN
		 	SET result_str = 'IV tube present';
		 END IF;
	 ELSEIF in_type = 'Tube status' THEN
		 IF in_string_parsed LIKE ('%Intubated%') OR in_string_parsed LIKE ('%Extubated%') OR in_string_parsed LIKE ('%Tracheostomy%') THEN
		 	SET result_str = 'IV status';
		 END IF;
	 ELSEIF in_type = 'Airway' THEN
		 IF in_string_parsed = 'Endotracheal' THEN
		  SET result_str = 'Endotracheal';
		 END IF;
	 ELSEIF in_type = 'Temperature (site)' THEN
		 IF in_string_parsed IN (SELECT source_text FROM COVID_SUPPLEMENT.TEXT_STANDARDIZATION WHERE sub_standard_meaning IN ('Core Temperature')) THEN
		 	SET result_str = 'Core Temperature';
		 ELSE
		 	SET result_str = 'Aux Temperature';
		 END IF;
    END IF;
    RETURN result_str;

END$$

DELIMITER ;


DROP FUNCTION REMAP.get_physio_result_str_epic;

DELIMITER $$

CREATE FUNCTION REMAP.get_physio_result_str_epic(in_type VARCHAR(32), in_string VARCHAR(255)) 
RETURNS VARCHAR(16)
NO SQL
BEGIN
    DECLARE result_str VARCHAR(16) DEFAULT '';
    DECLARE in_string_parsed VARCHAR(255);

    SET in_string_parsed = in_string;    

	 IF in_type = 'Oxygen therapy delivery device' THEN
		 IF in_string_parsed = 'High flow nasal' THEN		
			SET result_str = 'HFNC device';	
		 ELSEIF in_string_parsed IN ('BiPAP', 'CPAP') THEN 
			SET result_str = 'NIV device';	
	    ELSEIF in_string_parsed = 'Endotracheal tu' THEN
			SET result_str = 'IV device';	
		 ELSEIF in_string_parsed IN ('Nasal cannula') THEN
		  	SET result_str = 'NC';
		 ELSEIF in_string_parsed IN ('Simple mask') THEN
		  	SET result_str = 'Mask';
		 ELSEIF in_string_parsed IN ('Non-rebreather ') THEN
		  	SET result_str = 'Nonrebreather';
		 END IF; 
	 ELSEIF in_type IN ('Mode') THEN
		 IF in_string_parsed = 'PRVC/AC' THEN		
			SET result_str = 'IV mode';	
		 ELSEIF in_string_parsed IN ('BiPAP', 'CPAP') THEN 
			SET result_str = 'NIV mode';	
		 ELSEIF in_string_parsed REGEXP '^-?[0-9]+$' THEN
		  	SET result_str = 'NIV mode';
		 END IF; 
	 END IF;
    

    RETURN result_str;

END$$

DELIMITER ;

	
	
########################################################

DROP FUNCTION REMAP.convert_epic_unit;

DELIMITER $$

CREATE FUNCTION REMAP.convert_epic_unit(in_string VARCHAR(100)) 
RETURNS VARCHAR(16)
NO SQL
BEGIN
    DECLARE result_str VARCHAR(16) DEFAULT '';

	 IF in_string = 'Intensive Care' THEN		
		SET result_str = 'ICU';	
	 ELSEIF in_string IN ('Emergency Medicine') THEN 
		SET result_str = 'ED';	
    ELSEIF in_string = 'Admitting/Central Scheduling' THEN
		SET result_str = 'ignore';	
	 ELSEIF in_string IN ('Endoscopy') THEN
	  	SET result_str = 'ignore';
	 ELSEIF in_string IN ('Operating Room') THEN
	  	SET result_str = 'ignore';
	 ELSEIF in_string IN ('Observation') THEN
	  	SET result_str = 'Ward';
	 ELSEIF in_string IN ('Cardiology') THEN
	 	SET result_str = 'Ward';
	 END IF; 


    RETURN result_str;

END$$

DELIMITER ;
