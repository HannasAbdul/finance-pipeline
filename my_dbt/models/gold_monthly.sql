{{ config(materialized='table', schema='GOLD') }}
SELECT
  symbol,
  DATE_TRUNC('MONTH', price_date) as month,
  AVG(close_price) as avg_price
FROM {{ ref('silver_finance') }}
GROUP BY 1,2
