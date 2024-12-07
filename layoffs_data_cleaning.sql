-- Data Cleaning
-- Data for layoffs around the world in 2023

Goals
-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove any columns

-- Let's duplicate the original table data
CREATE TABLE world_layoffs.layoffs_staging
(LIKE world_layoffs.layoffs INCLUDING ALL);

SELECT * FROM world_layoffs.layoffs_staging;

INSERT INTO world_layoffs.layoffs_staging
SELECT * 
FROM world_layoffs.layoffs;

SELECT * FROM world_layoffs.layoffs_staging LIMIT 10;

-- Setting the Search Path
SET search_path TO world_layoffs;

SELECT * FROM layoffs_staging;

-- Step 1: Finding and Removing Duplicate Values
-- Let's find the dupliate values 
-- and use partition_by on every column to find duplicates
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY 
company, location, industry, 
total_laid_off, percentage_laid_off, 'date',
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1
order by company;

select * 
from layoffs_staging
where company='ExtraHop';

-- New Table to Insert into non-duplicate values
CREATE TABLE layoffs_staging2 (
    company VARCHAR(255),
    location VARCHAR(255),
    industry VARCHAR(255),
    total_laid_off INTEGER,
    percentage_laid_off DECIMAL(5,2),
    date DATE,
    stage VARCHAR(50),
    country VARCHAR(255),
    funds_raised_millions DECIMAL(10,2),
	row_num INT
);

-- Since we can't delete directly from cte lets create a copy table
SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY 
company, location, industry, 
total_laid_off, percentage_laid_off, 'date',
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;


-- Step 2: Standardizing Data

-- Remove space character from company column
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2 ORDER BY 1;

-- Fuzzy Replacements
SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry='Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT(industry)
FROM layoffs_staging2;

-- Let's look at location
SELECT DISTINCT(location)
FROM layoffs_staging2;

-- Let's look at country
SELECT DISTINCT(country)
FROM layoffs_staging2
order by country;

SELECT DISTINCT(country)
FROM layoffs_staging2
WHERE country LIKE 'United States%';

-- Solution 1
UPDATE layoffs_staging2
SET country='United States'
WHERE country LIKE 'United States%';

-- Solution 2
SELECT DISTINCT country, 
TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

select count(*) from
layoffs_staging2
where country = 'United States';


SET search_path TO world_layoffs;
select * from world_layoffs.layoffs_staging2;

SELECT date
FROM layoffs_staging2;

-- Check the columns
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'layoffs_staging2';

-- Date
-- Should be a Date type

SELECT date,
TO_DATE(date, 'MM/DD/YY')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET date = TO_DATE(date, 'MM/DD/YYYY')

/* SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'layoffs_staging2' AND column_name = 'date'; */

-- Change the data type of date column to Date
-- Also attept to cast the date to the DATE type
ALTER TABLE layoffs_staging2
ALTER COLUMN date TYPE DATE USING date::DATE;

-- Looking at null values
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry ='';

-- In some instances data can be substituted 
-- Example, for this case - industry
SELECT * 
FROM layoffs_staging2
WHERE company='Airbnb';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
SET industry = t2.industry
FROM layoffs_staging2 t2
WHERE t1.company = t2.company
	AND (t1.industry IS NULL OR t1.industry ='')
	AND t2.industry IS NOT NULL;

-- Check
SELECT * FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

SELECT * FROM layoffs_staging2 
WHERE company LIKE 'Bally%';

-- Drop column row_num
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;