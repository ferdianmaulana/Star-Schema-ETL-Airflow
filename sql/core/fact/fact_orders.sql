-- Update fact_orders table in CORE layer
-- This query assumes the table already exists and is partitioned by order_date
-- Only process data from the specified partition date (DSTART)
-- Using MERGE statement for upsert operations (insert new, update existing)

MERGE INTO `{{project_id}}.{{core_dataset}}.fact_orders` T
USING (
  -- Source data at transactional level (no aggregation)
  WITH source_data AS (
    SELECT
      o.order_id,
      o.order_date,
      o.customer_id,
      o.amount AS order_amount,
      o.status,
      o.created_at,
      oi.order_item_id,
      oi.product_id,
      oi.quantity,
      oi.unit_price,
      oi.item_amount
    FROM 
      `{{project_id}}.{{raw_dataset}}.orders` o
    JOIN
      `{{project_id}}.{{raw_dataset}}.order_items` oi
    ON 
      o.order_id = oi.order_id
    -- Only process data for the specified date
    WHERE DATE(o.ingestion_timestamp) = DATE('{{dstart}}')
  )
  SELECT
    sd.order_id,
    sd.order_date,
    dc.customer_sk,
    dp.product_sk,
    sd.order_amount,
    sd.quantity,
    sd.order_item_id,
    sd.unit_price,
    sd.item_amount,
    sd.status,
    sd.created_at,
    CURRENT_TIMESTAMP() AS updated_at
  FROM
    source_data sd
  JOIN
    `{{project_id}}.{{core_dataset}}.dim_customers` dc
  ON
    sd.customer_id = dc.customer_id
    AND sd.order_date >= dc.effective_date
    AND (dc.expiration_date IS NULL OR sd.order_date < dc.expiration_date)
  JOIN
    `{{project_id}}.{{core_dataset}}.dim_products` dp
  ON
    sd.product_id = dp.product_id
    AND sd.order_date >= dp.effective_date
    AND (dp.expiration_date IS NULL OR sd.order_date < dp.expiration_date)
) S
ON T.order_id = S.order_id AND T.order_item_id = S.order_item_id

-- When matched, update the record
WHEN MATCHED THEN
  UPDATE SET
    order_date = S.order_date,
    customer_sk = S.customer_sk,
    order_amount = S.order_amount,
    quantity = S.quantity,
    product_id = S.product_id,
    product_sk = S.product_sk,
    unit_price = S.unit_price,
    item_amount = S.item_amount,
    status = S.status,
    updated_at = S.updated_at

-- When not matched, insert a new record
WHEN NOT MATCHED THEN
  INSERT (
    order_id,
    order_date,
    customer_sk,
    order_amount,
    quantity,
    order_item_id,
    product_id,
    product_sk,
    unit_price,
    item_amount,
    status,
    created_at,
    updated_at
  )
  VALUES (
    S.order_id,
    S.order_date,
    S.customer_sk,
    S.order_amount,
    S.quantity,
    S.order_item_id,
    S.product_id,
    S.unit_price,
    S.item_amount,
    S.status,
    S.created_at,
    S.updated_at
  );
