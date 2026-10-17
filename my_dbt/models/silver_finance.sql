{{ config(materialized='table') }}
SELECT
  symbol,
  price_date::DATE as price_date,
  close_price::FLOAT as close_price,
  volume::INT as volume
FROM {{ source('bronze', 'finance_raw') }}
WHERE close_price > 0
