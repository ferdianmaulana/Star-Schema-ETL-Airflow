-- Update sales_summary table in DATAMART layer
-- This query assumes the table already exists and is partitioned by date
-- Only process data for the specified partition date (DSTART)

-- Delete data for the specified date to allow for a refresh
DELETE FROM `{{project_id}}.{{datamart_dataset}}.sales_summary`
WHERE date = DATE('{{dstart}}');

-- Insert data for the specified date
INSERT INTO `{{project_id}}.{{datamart_dataset}}.sales_summary` (
  date,
  year,
  month,
  month_name,
  product_category,
  total_sales,
  total_orders,
  total_quantity,
  avg_order_value,
  created_at
)
WITH order_items_detail AS (
  SELECT
    fo.order_id,
    fo.order_date AS date,
    dp.category AS product_category,
    fo.order_amount,
    fo.quantity
  FROM
    `{{project_id}}.{{core_dataset}}.fact_orders` fo
  JOIN
    `{{project_id}}.{{core_dataset}}.dim_products` dp
  ON
    fo.product_sk = dp.product_sk
  -- Only process data for the specified date
  WHERE fo.order_date = DATE('{{dstart}}')
)
SELECT
  oid.date,
  dd.year,
  dd.month,
  dd.month_name,
  oid.product_category,
  SUM(oid.order_amount) AS total_sales,
  COUNT(DISTINCT oid.order_id) AS total_orders,
  SUM(oid.quantity) AS total_quantity,
  SAFE_DIVIDE(SUM(oid.order_amount), COUNT(DISTINCT oid.order_id)) AS avg_order_value,
  CURRENT_TIMESTAMP() AS created_at
FROM
  order_items_detail oid
JOIN
  `{{project_id}}.{{core_dataset}}.dim_dates` dd
ON
  oid.date = dd.full_date
GROUP BY
  oid.date,
  dd.year,
  dd.month,
  dd.month_name,
  oid.product_category;
