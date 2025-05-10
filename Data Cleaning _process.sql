-- DATA CLEANING IN SQL
-- STEPS : 1. REMOVE DUPLICATES
--         2. STANDARDIZE THE DATA
--         3. REMOVE NULL VALUES AND BLANK VALUES
--         4. REMOVE ANY COLUMNS AND ROWS WHICH ARE NOT NECESSARY


select count(*) from layoffs;

select * from layoffs;

-- pre-req. Create Staging table for backup-- 

Create table staging
like layoffs;

-- Insert data into Staging table
Insert into staging
select * from layoffs;

select * from staging;

select count(*) from staging;

-- 1. Remove the duplicates-- 

select *, row_number() OVER (
partition by COMPANY, LOCATION, INDUSTRY, total_laid_off , percentage_laid_off , `DATE` , STAGE,  COUNTRY, funds_raised_millions )
AS ROW_NUM from staging 
ORDER BY ROW_NUM DESC;

WITH duplicate_cte as 
(select *, row_number() OVER (
partition by COMPANY, LOCATION, INDUSTRY, total_laid_off , percentage_laid_off , `DATE` , STAGE,  COUNTRY, funds_raised_millions )
AS ROW_NUM from staging)
delete from duplicate_cte
where row_num >1;

-- --we cannot able to delete over CTE so we are creating another table and adding row_num column extra and removing 
-- dublicates form that table 

CREATE TABLE `staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `ROW_NUM` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --staging table 2 is created now , insert the CTE data into staging2  
select * from staging2;

insert into staging2
select *, row_number() OVER (
partition by COMPANY, LOCATION, INDUSTRY, total_laid_off , percentage_laid_off , `DATE` , STAGE,  COUNTRY, funds_raised_millions )
AS ROW_NUM from staging;

-- Row_num is added  ,now we can remove duplicates easily-- 

SELECT * FROM staging2
WHERE ROW_NUM >1;

DELETE FROM STAGING2
WHERE ROW_NUM>1;

-- --2. STANDARDIZING THE DATA--

-- COMPANY COLUMN
SELECT company FROM staging2;

UPDATE staging2
SET COMPANY = trim(COMPANY);

-- INDUSTRY COLUMN
SELECT DISTINCT INDUSTRY FROM staging2
ORDER BY 1 ;

SELECT INDUSTRY FROM staging2
WHERE INDUSTRY LIKE "CRYPTO%";

UPDATE STAGING2
SET INDUSTRY = "CRYPTO"
WHERE INDUSTRY LIKE "CRYPTO%";

-- LOCATION COLUMN
SELECT * FROM STAGING2;

SELECT DISTINCT LOCATION
FROM staging2
ORDER BY 1;

-- COUNTRY COLUMN
SELECT distinct COUNTRY
FROM staging2
ORDER BY 1;

SELECT DISTINCT COUNTRY FROM STAGING2
WHERE COUNTRY LIKE "UNITED STATES%";

UPDATE staging2
SET COUNTRY = "United States"
WHERE COUNTRY ="United States.";

-- STAGE COLUMN-- 
SELECT * FROM STAGING2;
SELECT DISTINCT STAGE FROM staging
ORDER BY 1;

-- DATE COLUMN
SELECT distinct `DATE` FROM STAGING2
ORDER BY 1;

SELECT `DATE` , str_to_date(`DATE`, '%m/%d/%Y') FROM staging2;

UPDATE STAGING2
SET `DATE` = TRIM(str_to_date(`DATE`, '%m/%d/%Y'));

SELECT `DATE` FROM STAGING2;

-- CHANGING THE DATA TYPE OF THE DATA ITS TEXT FORMAT NOW CHANGING TO DATE FORMAT
ALTER TABLE STAGING2
CHANGE `DATE` `DATE` DATE;

-- SOME OF INDUSTRY VALUES ARE NULL FOR SAME COMPANY UPDATE THE CORRECT INDUSTRY FOR NULL OR BLANK 

select * from staging2
WHERE INDUSTRY IS NULL OR INDUSTRY = '';

UPDATE STAGING2
SET INDUSTRY  = null
WHERE INDUSTRY = '';

-- FINDING THE INDUSTRY HAVE BLANK IN T1 BUT NOT IN T2 FOR THR SAME COMPANY-- 

SELECT T1.COMPANY, T1.INDUSTRY , T2.INDUSTRY
FROM STAGING2 T1
JOIN STAGING2 T2
   ON T1.COMPANY = T2.COMPANY
WHERE (T1.INDUSTRY IS NULL OR T1.INDUSTRY = '')
     AND T2.INDUSTRY IS NOT NULL;
     
-- UPDATING T1 INDUSTRY VALUE TO T2 INDUSTRY VALUES 

UPDATE staging2 T1
JOIN staging2 T2 
	ON T1.COMPANY = T2.COMPANY
SET T1.INDUSTRY = T2.INDUSTRY 
WHERE T1.INDUSTRY IS NULL
AND T2.INDUSTRY IS NOT NULL;

SELECT * FROM STAGING2
WHERE COMPANY = 'AIRBNB';

-- 3. REMOVE THE ROWS FOR PERCENTAGE_LAID OFF AND TOTAL_LAID_OFF ARE BLANK

SELECT * FROM STAGING2
WHERE percentage_laid_off IS NULL AND total_laid_oFF IS NULL;

DELETE FROM STAGING2
WHERE percentage_laid_off IS NULL AND total_laid_oFF IS NULL; 

-- 4. REMOVE THE ROWNUM COLUMN WHICH IS NOT NECESSARY NOW
ALTER TABLE STAGING2
DROP COLUMN ROW_NUM;

SELECT * FROM STAGING2
where `date` is null;

commit;



