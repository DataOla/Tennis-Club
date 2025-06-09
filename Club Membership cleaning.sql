SELECT *
FROM member_info;

--- renaming a column with a spelling error ---

ALTER TABLE member_info
RENAME COLUMN martial_status to marital_status;

--- created a staging table avoid altering the raw data ---

CREATE TABLE member_info_staging
LIKE member_info;

INSERT member_info_staging
SELECT *
FROM member_info;

SELECT *
FROM member_info_staging;

--- Finding duplicates ---

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY full_name, age, email, phone, full_address, job_title, membership_date) AS row_num
FROM member_info_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY full_name, age, email, phone, full_address, job_title, membership_date) AS row_num
FROM member_info_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1
;

SELECT *
FROM member_info_staging
WHERE full_name LIKE 'georges prewett';

--- created a new table with the row_num column to clean the data properly ---

CREATE TABLE `member_info_staging2` (
  `full_name` text,
  `age` int DEFAULT NULL,
  `marital_status` text,
  `email` text,
  `phone` text,
  `full_address` text,
  `job_title` text,
  `membership_date` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--- Inserted everything from the CTE ---

INSERT INTO member_info_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY full_name, age, email, phone, full_address, job_title, membership_date) AS row_num
FROM member_info_staging
;

--- Deleted rows with duplicates ---

DELETE
FROM member_info_staging2
WHERE row_num > 1;

--- Checking all columns to standardize data ---

SELECT COUNT(DISTINCT(full_name))
FROM member_info_staging2;

SELECT COUNT(full_name)
FROM member_info_staging2;

SELECT full_name,
ROW_NUMBER() OVER(PARTITION BY full_name) AS row_num2
FROM member_info_staging2;

WITH name_dup AS
(
SELECT full_name,
ROW_NUMBER() OVER(PARTITION BY full_name) AS row_num2
FROM member_info_staging2
)
SELECT *
FROM name_dup
WHERE row_num2 > 1;

SELECT *
FROM member_info_staging2
WHERE full_name LIKE 'Haskell Braden';

SELECT DISTINCT(phone)
FROM member_info_staging2;

WITH phone_dup AS
(
SELECT phone,
ROW_NUMBER() OVER(PARTITION BY phone) AS row_num2
FROM member_info_staging2
)
SELECT *
FROM phone_dup
WHERE row_num2 > 1;

SELECT *
FROM member_info_staging2;

--- Checking and fixing inconsistent values ---

SELECT full_name
FROM member_info_staging2;

UPDATE member_info_staging2
SET full_name = TRIM(full_name);

UPDATE member_info_staging2
SET marital_status = TRIM(marital_status),
    job_title = TRIM(job_title)
    ;
    
--- fixing the error in the full_name colun ---

SELECT *
FROM member_info_staging2
WHERE full_name LIKE '%???%'
;

SELECT full_name, REGEXP_REPLACE(full_name, '^\\?{3}','') AS names
FROM member_info_staging2;

UPDATE member_info_staging2
SET full_name = REGEXP_REPLACE(full_name, '^\\?{3}','');

--- fixing inconsistent sentence case ---

UPDATE member_info_staging2
SET full_name = LCASE(full_name);

--- fixing misspelled entries in the marital_status column

SELECT COUNT(*) AS Total,
SUM(case when marital_status = 'separated' then 1 else 0 end) AS separated,
SUM(case when marital_status = 'single' then 1 else 0 end) AS single,
SUM(case when marital_status = 'married' then 1 else 0 end) AS married,
SUM(case when marital_status = 'divorced' then 1 else 0 end) AS divorced,
SUM(case when marital_status = '' then 1 else 0 end) AS blank
FROM member_info_staging2;

SELECT DISTINCT(marital_status)
FROM member_info_staging2;

SELECT *
FROM member_info_staging2
WHERE marital_status LIKE 'divored';

UPDATE member_info_staging2
SET marital_status = 'divorced'
WHERE marital_status = 'divored';

--- Fixing date string

SELECT membership_date,
STR_TO_DATE(membership_date, '%m/%d/%Y')
FROM member_info_staging2;

UPDATE member_info_staging2
SET membership_date = STR_TO_DATE(membership_date, '%m/%d/%Y');

ALTER TABLE member_info_staging2
MODIFY membership_date DATE;

--- Some dates in the membership_date column are in the 1900s so I am updating the dates by adding 100 years ---

SELECT *
FROM member_info_staging2
WHERE YEAR(membership_date) < 2000;

UPDATE member_info_staging2
SET membership_date = DATE_ADD(membership_date, INTERVAL 100 YEAR)
WHERE YEAR(membership_date) < 2000;

--- Some ages have an additional digit at the end so I removed the last digit

SELECT *
FROM member_info_staging
WHERE age LIKE '___';

SELECT age, TRIM(TRAILING '_' FROM age)
FROM member_info_staging2;

SELECT age,
CASE 
	WHEN LENGTH(CAST(age AS CHAR)) > 2 THEN CAST(LEFT(CAST(age AS CHAR), LENGTH(CAST(age AS CHAR)) - 1) AS UNSIGNED)
    ELSE age
    END
FROM member_info_staging2;

UPDATE member_info_staging2
SET age = CASE 
			WHEN LENGTH(CAST(age AS CHAR)) > 2 THEN CAST(LEFT(CAST(age AS CHAR), LENGTH(CAST(age AS CHAR)) - 1) AS UNSIGNED)
			ELSE age
			END
;

--- Spliting the full address into individual street address, city and state ---

SELECT 
  SUBSTRING_INDEX(full_address, ',', 1) AS street_address,
  SUBSTRING_INDEX(SUBSTRING_INDEX(full_address, ',', 2), ',', -1) AS city,
  SUBSTRING_INDEX(full_address, ',', -1) AS state
FROM member_info_staging2;

ALTER TABLE member_info_staging2
ADD COLUMN street_address VARCHAR(100),
ADD COLUMN city VARCHAR(50),
ADD COLUMN state VARCHAR(50);

UPDATE member_info_staging2
SET 
	street_address = SUBSTRING_INDEX(full_address, ',', 1),
	city = SUBSTRING_INDEX(SUBSTRING_INDEX(full_address, ',', 2), ',', -1),
	state = SUBSTRING_INDEX(full_address, ',', -1);
    
SELECT *
FROM member_info_staging2;

--- Fixing Blank and Null values---

SELECT *
FROM member_info_staging2
WHERE job_title = '';

UPDATE member_info_staging2
SET phone = NULL
WHERE phone = '';

--- Setting incomplete phone numbers to null ---

SELECT *
FROM member_info_staging2
WHERE length(phone) < 12;
    
UPDATE member_info_staging2
SET phone = NULL
WHERE length(phone) < 12;
    
--- Setting columns without marital status to null ---

SELECT *
FROM member_info_staging2
WHERE marital_status = '';

SELECT *
FROM member_info_staging2
WHERE marital_status = null;

UPDATE member_info_staging2
SET marital_status = null
WHERE marital_status = '';

--- Removing the redundant full_name and row_num columns ---

ALTER TABLE member_info_staging2
DROP COLUMN full_address,
DROP COLUMN row_num;

SELECT DISTINCT(state)
FROM member_info_staging2
ORDER BY 1;

--- Trimming and fixing spell errors in the state column ---

UPDATE member_info_staging2
SET state = TRIM(state);

UPDATE member_info_staging2
SET state = 'California' WHERE state = 'Kalifornia';

UPDATE member_info_staging2
SET state = 'District of Columbia' WHERE state = 'Districts of Columbia';

UPDATE member_info_staging2
SET state = 'Kansas' WHERE state = 'Kansus';

UPDATE member_info_staging2
SET state = 'New York' WHERE state = 'NewYork';

UPDATE member_info_staging2
SET state = 'North Carolina' WHERE state = 'NorthCarolina';

UPDATE member_info_staging2
SET state = 'South Dakota' WHERE state = 'South Dakotaaa';

UPDATE member_info_staging2
SET state = 'Tejas' WHERE state = 'Tej+F823as';

UPDATE member_info_staging2
SET state = 'Tennessee' WHERE state = 'Tennesseeee';

SELECT DISTINCT(city)
FROM member_info_staging2
ORDER BY 1;
