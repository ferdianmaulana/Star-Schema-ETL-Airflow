-- Update dim_customers table in CORE layer (SCD Type 2)
-- This query assumes the table already exists and is partitioned by effective_date
-- Only process data from the specified partition date (DSTART)

-- First, get the latest surrogate key to continue the sequence
DECLARE max_customer_sk INT64 DEFAULT (
  SELECT COALESCE(MAX(customer_sk), 0)
  FROM `{{project_id}}.{{core_dataset}}.dim_customers`
);

-- Insert/update records using SCD Type 2 methodology with improved MERGE
MERGE INTO `{{project_id}}.{{core_dataset}}.dim_customers` AS target
USING (
  -- Source data from raw layer
  WITH source_data AS (
    SELECT
      customer_id,
      first_name,
      last_name,
      email,
      address,
      city,
      state,
      zipcode,
      created_at
    FROM `{{project_id}}.{{raw_dataset}}.customers` AS raw
    -- Only process data for the specified date
    WHERE DATE(ingestion_timestamp) = DATE('{{dstart}}')
  )
  
  -- Compare with current records to identify changes
  SELECT
    s.customer_id,
    s.first_name,
    s.last_name,
    s.email,
    s.address,
    s.city,
    s.state,
    s.zipcode,
    s.created_at,
    t.customer_sk,
    -- Check if record exists and has changes
    CASE 
      WHEN t.customer_sk IS NULL THEN FALSE  -- New record
      WHEN t.first_name != s.first_name OR 
           t.last_name != s.last_name OR 
           t.email != s.email OR 
           t.address != s.address OR 
           t.city != s.city OR 
           t.state != s.state OR 
           t.zipcode != s.zipcode THEN TRUE  -- Existing record with changes
      ELSE FALSE  -- Existing record with no changes
    END AS has_changes
  FROM source_data AS s
  LEFT JOIN `{{project_id}}.{{core_dataset}}.dim_customers` AS t
    ON s.customer_id = t.customer_id
    AND t.is_current = TRUE
) AS source
ON target.customer_id = source.customer_id AND target.is_current = TRUE

-- For existing records that have changes, expire the current record
WHEN MATCHED AND source.has_changes THEN
  UPDATE SET
    expiration_date = CURRENT_DATE(),
    is_current = FALSE,
    updated_at = CURRENT_TIMESTAMP()

-- For new records, insert them
WHEN NOT MATCHED THEN
  INSERT (
    customer_sk, customer_id, first_name, last_name, email, address, city, state, zipcode,
    effective_date, expiration_date, is_current, created_at, updated_at
  )
  VALUES (
    max_customer_sk + ROW_NUMBER() OVER(), 
    source.customer_id, source.first_name, source.last_name, source.email, 
    source.address, source.city, source.state, source.zipcode,
    CURRENT_DATE(), NULL, TRUE, source.created_at, CURRENT_TIMESTAMP()
  );

-- Insert new versions of records that have changes
INSERT INTO `{{project_id}}.{{core_dataset}}.dim_customers` (
  customer_sk, customer_id, first_name, last_name, email, address, city, state, zipcode,
  effective_date, expiration_date, is_current, created_at, updated_at
)
SELECT
  max_customer_sk + ROW_NUMBER() OVER() + (
    SELECT COUNT(*) FROM `{{project_id}}.{{core_dataset}}.dim_customers` AS t
    LEFT JOIN `{{project_id}}.{{raw_dataset}}.customers` AS s
    ON t.customer_id = s.customer_id AND t.is_current = TRUE
    WHERE DATE(s.ingestion_timestamp) = DATE('{{dstart}}')
    AND t.customer_id IS NOT NULL
    AND (
      t.first_name != s.first_name OR 
      t.last_name != s.last_name OR 
      t.email != s.email OR 
      t.address != s.address OR 
      t.city != s.city OR 
      t.state != s.state OR 
      t.zipcode != s.zipcode
    )
  ),
  s.customer_id, s.first_name, s.last_name, s.email, s.address, s.city, s.state, s.zipcode,
  CURRENT_DATE(), NULL, TRUE, s.created_at, CURRENT_TIMESTAMP()
FROM `{{project_id}}.{{raw_dataset}}.customers` AS s
JOIN `{{project_id}}.{{core_dataset}}.dim_customers` AS t
  ON s.customer_id = t.customer_id
  AND t.is_current = FALSE  -- Join with records that were just expired
  AND t.expiration_date = CURRENT_DATE()  -- Only get records expired today
WHERE DATE(s.ingestion_timestamp) = DATE('{{dstart}}');

-- Insert new versions of updated records
INSERT INTO `{{project_id}}.{{core_dataset}}.dim_customers` (
  customer_sk, customer_id, first_name, last_name, email, address, city, state, zipcode,
  effective_date, expiration_date, is_current, created_at, updated_at
)
SELECT
  (SELECT MAX(customer_sk) FROM `{{project_id}}.{{core_dataset}}.dim_customers`) + ROW_NUMBER() OVER() AS customer_sk,
  source.customer_id,
  source.first_name,
  source.last_name,
  source.email,
  source.address,
  source.city,
  source.state,
  source.zipcode,
  CURRENT_DATE() AS effective_date,
  NULL AS expiration_date,
  TRUE AS is_current,
  source.created_at,
  CURRENT_TIMESTAMP() AS updated_at
FROM (
  SELECT
    s.customer_id,
    s.first_name,
    s.last_name,
    s.email,
    s.address,
    s.city,
    s.state,
    s.zipcode,
    s.created_at
  FROM `{{project_id}}.{{raw_dataset}}.customers` AS s
  JOIN `{{project_id}}.{{core_dataset}}.dim_customers` AS t
    ON s.customer_id = t.customer_id
    AND t.is_current = FALSE
    AND t.expiration_date = CURRENT_DATE()  -- Only the ones we just expired
  WHERE 
    s.first_name != t.first_name OR 
    s.last_name != t.last_name OR 
    s.email != t.email OR 
    s.address != t.address OR 
    s.city != t.city OR 
    s.state != t.state OR 
    s.zipcode != t.zipcode
) AS source;
