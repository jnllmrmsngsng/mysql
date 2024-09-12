-- DATA CLEANING

SELECT * FROM layoffs;


-- 1. remove duplicates
-- 2. standardized data
-- 3. null values or blank values
-- 4. remove any columns

-- copy raw data from raw table
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

INSERT layoffs_staging
SELECT * FROM layoffs;



-- 1. remove duplicates
SELECT *, ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num 
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *, ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging
)
SELECT * FROM duplicate_cte
WHERE row_num > 1;

SELECT * FROM layoffs_staging WHERE company = 'Casper';



WITH duplicate_cte AS
(
SELECT *, ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging
)
SELECT * FROM duplicate_cte
WHERE row_num > 1;

-- creating table since cte can't be updatable
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging;

DELETE FROM layoffs_staging2
WHERE row_num > 1;

SELECT * FROM layoffs_staging2;


-- standardized data
-- removing white spaces
SELECT company, TRIM(company) FROM layoffs_staging2;


UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT industry FROM layoffs_staging2;

-- changing cryptocurrency to just crypto
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- removing period at the end of country
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) FROM layoffs_staging2 ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- formatting date
SELECT `date`, 
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET date = STR_TO_DATE(`date`, '%m/%d/%Y');

 
 SELECT * FROM layoffs_staging2;
 
 -- changing into date column, staging
 
 ALTER TABLE layoffs_staging2
 MODIFY COLUMN `date` DATE;
 
 
 -- 3. working with null values
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;
-- set blanks to null
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT * FROM layoffs_staging2 WHERE industry IS NULL OR industry = '';

SELECT * FROM layoffs_staging2 WHERE company = 'Airbnb';

-- add an industry to airbnb blank

SELECT t1.industry, t2.industry FROM layoffs_staging2 t1
JOIN  layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
WHERE (t1.industry  IS NULL OR t1.industry = '') AND t2.industry IS NOT NULL;


 -- update 
 UPDATE layoffs_staging2 t1
 JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry is NULL
AND t2.industry IS NOT NULL;


-- 4. removing columns

SELECT * FROM layoffs_staging2;

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- DROP A COLUMN
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
