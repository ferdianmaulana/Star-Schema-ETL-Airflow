-- Update customer_analytics table in DATAMART layer
-- This query assumes the table already exists
-- We'll refresh all customer data for the execution date (DSTART)

-- Delete all data to allow for a refresh (since this is a full refresh table)
DELETE FROM `{{project_id}}.{{datamart_dataset}}.customer_analytics`
WHERE TRUE;

-- Insert data for all customers, using the execution date for the analysis
INSERT INTO `{{project_id}}.{{datamart_dataset}}.customer_analytics` (
  customer_id,
  first_name,
  last_name,
  city,
  state,
  first_order_date,
  last_order_date,
  days_since_last_order,
  total_orders,
  total_lifetime_value,
  average_order_value,
  customer_segment,
  created_at
)
WITH customer_orders AS (
  SELECT
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    dc.city,
    dc.state,
    MIN(fo.order_date) AS first_order_date,
    MAX(fo.order_date) AS last_order_date,
    DATE_DIFF(DATE('{{dstart}}'), MAX(fo.order_date), DAY) AS days_since_last_order,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fo.item_amount) AS total_lifetime_value
  FROM
    `{{project_id}}.{{core_dataset}}.fact_orders` fo
  JOIN
    `{{project_id}}.{{core_dataset}}.dim_customers` dc
  ON
    fo.customer_sk = dc.customer_sk
  WHERE
    dc.is_current = TRUE  -- For customer analytics, we want the current customer profile
  GROUP BY
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    dc.city,
    dc.state
)
SELECT
  co.customer_id,
  co.first_name,
  co.last_name,
  co.city,
  co.state,
  co.first_order_date,
  co.last_order_date,
  co.days_since_last_order,
  co.total_orders,
  co.total_lifetime_value,
  SAFE_DIVIDE(co.total_lifetime_value, co.total_orders) AS average_order_value,
  -- Simple customer segmentation based on RFM (Recency, Frequency, Monetary)
  CASE
    WHEN co.days_since_last_order <= 30 AND co.total_orders >= 3 AND co.total_lifetime_value >= 500 THEN 'VIP'
    WHEN co.days_since_last_order <= 90 AND co.total_orders >= 2 THEN 'Loyal'
    WHEN co.days_since_last_order <= 180 THEN 'Active'
    WHEN co.days_since_last_order <= 365 THEN 'At Risk'
    ELSE 'Inactive'
  END AS customer_segment,
  CURRENT_TIMESTAMP() AS created_at
FROM
  customer_orders co;
