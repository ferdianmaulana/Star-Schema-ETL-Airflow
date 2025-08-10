-- Update dim_products table in CORE layer (SCD Type 2)
-- This query assumes the table already exists and is partitioned by effective_date
-- Only process data from the specified partition date (DSTART)

-- First, get the latest surrogate key to continue the sequence
DECLARE max_product_sk INT64 DEFAULT (
  SELECT COALESCE(MAX(product_sk), 0)
  FROM `{{project_id}}.{{core_dataset}}.dim_products`
);

-- Insert/update records using SCD Type 2 methodology with improved MERGE
MERGE INTO `{{project_id}}.{{core_dataset}}.dim_products` AS target
USING (
  -- Source data from raw layer
  WITH source_data AS (
    SELECT
      product_id,
      name,
      category,
      price,
      created_at
    FROM `{{project_id}}.{{raw_dataset}}.products` AS raw
    -- Only process data for the specified date
    WHERE DATE(ingestion_timestamp) = DATE('{{dstart}}')
  )
  
  -- Compare with current records to identify changes
  SELECT
    s.product_id,
    s.name,
    s.category,
    s.price,
    s.created_at,
    t.product_sk,
    -- Check if record exists and has changes
    CASE 
      WHEN t.product_sk IS NULL THEN FALSE  -- New record
      WHEN t.name != s.name OR 
           t.category != s.category OR 
           t.price != s.price THEN TRUE  -- Existing record with changes
      ELSE FALSE  -- Existing record with no changes
    END AS has_changes
  FROM source_data AS s
  LEFT JOIN `{{project_id}}.{{core_dataset}}.dim_products` AS t
    ON s.product_id = t.product_id
    AND t.is_current = TRUE
) AS source
ON target.product_id = source.product_id AND target.is_current = TRUE

-- For existing records that have changes, expire the current record
WHEN MATCHED AND source.has_changes THEN
  UPDATE SET
    expiration_date = CURRENT_DATE(),
    is_current = FALSE,
    updated_at = CURRENT_TIMESTAMP()

-- For new records, insert them
WHEN NOT MATCHED THEN
  INSERT (
    product_sk, product_id, name, category, price,
    effective_date, expiration_date, is_current, created_at, updated_at
  )
  VALUES (
    max_product_sk + ROW_NUMBER() OVER(), 
    source.product_id, source.name, source.category, source.price,
    CURRENT_DATE(), NULL, TRUE, source.created_at, CURRENT_TIMESTAMP()
  );

-- Insert new versions of records that have changes
INSERT INTO `{{project_id}}.{{core_dataset}}.dim_products` (
  product_sk, product_id, name, category, price,
  effective_date, expiration_date, is_current, created_at, updated_at
)
SELECT
  max_product_sk + ROW_NUMBER() OVER() + (
    SELECT COUNT(*) FROM `{{project_id}}.{{core_dataset}}.dim_products` AS t
    LEFT JOIN `{{project_id}}.{{raw_dataset}}.products` AS s
    ON t.product_id = s.product_id AND t.is_current = TRUE
    WHERE DATE(s.ingestion_timestamp) = DATE('{{dstart}}')
    AND t.product_id IS NOT NULL
    AND (
      t.name != s.name OR 
      t.category != s.category OR 
      t.price != s.price
    )
  ),
  s.product_id, s.name, s.category, s.price,
  CURRENT_DATE(), NULL, TRUE, s.created_at, CURRENT_TIMESTAMP()
FROM `{{project_id}}.{{raw_dataset}}.products` AS s
JOIN `{{project_id}}.{{core_dataset}}.dim_products` AS t
  ON s.product_id = t.product_id
  AND t.is_current = FALSE  -- Join with records that were just expired
  AND t.expiration_date = CURRENT_DATE()  -- Only get records expired today
WHERE DATE(s.ingestion_timestamp) = DATE('{{dstart}}');
