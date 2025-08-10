-- Update dim_dates table in CORE layer
-- This query refreshes dates for the specified date (DSTART) plus 3 years

-- The dimension table is assumed to already exist
-- Using MERGE statement to handle date dimension updates
MERGE INTO `{{project_id}}.{{core_dataset}}.dim_dates` AS target
USING (
  -- Generate date range from execution date to 3 years in the future
  WITH date_range AS (
    SELECT
      date
    FROM
      UNNEST(GENERATE_DATE_ARRAY(
        DATE('{{dstart}}'),  -- Start from execution date
        DATE_ADD(DATE('{{dstart}}'), INTERVAL 3 YEAR),  -- End date (3 years in the future)
        INTERVAL 1 DAY
      )) AS date
  )
  
  -- Create the full dimension attributes for each date
  SELECT
    PARSE_DATE('%Y%m%d', FORMAT_DATE('%Y%m%d', date)) AS date_id,
    date AS full_date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(QUARTER FROM date) AS quarter,
    EXTRACT(MONTH FROM date) AS month,
    FORMAT_DATE('%B', date) AS month_name,
    EXTRACT(WEEK FROM date) AS week_of_year,
    EXTRACT(DAY FROM date) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
    FORMAT_DATE('%A', date) AS day_name,
    CASE
      WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN TRUE
      ELSE FALSE
    END AS is_weekend,
    -- Fiscal year (assuming fiscal year starts on July 1)
    CASE
      WHEN EXTRACT(MONTH FROM date) >= 7 THEN EXTRACT(YEAR FROM date)
      ELSE EXTRACT(YEAR FROM date) - 1
    END AS fiscal_year,
    -- Holiday flag (simplified example - you would add more holidays as needed)
    CASE
      WHEN FORMAT_DATE('%m-%d', date) = '01-01' THEN 'New Year''s Day'
      WHEN FORMAT_DATE('%m-%d', date) = '07-04' THEN 'Independence Day'
      WHEN FORMAT_DATE('%m-%d', date) = '12-25' THEN 'Christmas Day'
      ELSE NULL
    END AS holiday_name,
    CASE
      WHEN FORMAT_DATE('%m-%d', date) IN ('01-01', '07-04', '12-25') THEN TRUE
      ELSE FALSE
    END AS is_holiday,
    CURRENT_TIMESTAMP() AS created_at
  FROM date_range
) AS source
ON target.full_date = source.full_date

-- When matched, update the record if needed
-- Note: For date dimensions, we typically don't update existing records
-- but included here for completeness
WHEN MATCHED THEN
  UPDATE SET
    holiday_name = source.holiday_name,
    is_holiday = source.is_holiday

-- When not matched, insert a new record
WHEN NOT MATCHED THEN
  INSERT (
    date_id, full_date, year, quarter, month, month_name, 
    week_of_year, day_of_month, day_of_week, day_name, 
    is_weekend, fiscal_year, holiday_name, is_holiday, created_at
  )
  VALUES (
    source.date_id, source.full_date, source.year, source.quarter, 
    source.month, source.month_name, source.week_of_year, 
    source.day_of_month, source.day_of_week, source.day_name, 
    source.is_weekend, source.fiscal_year, source.holiday_name, 
    source.is_holiday, source.created_at
  );
