
{{
    config(
      materialized = 'table'
      )
}}

SELECT
  id_brand, brand, created_at
FROM {{ source('cars_src', 'brands_raw') }}
  