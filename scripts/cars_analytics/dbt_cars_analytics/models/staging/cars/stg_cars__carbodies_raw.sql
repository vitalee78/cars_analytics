{{
    config(
      materialized = 'table'
      )
}}

SELECT
  id_carbody, 
  id_model, 
  carbody, 
  description, 
  created_at
FROM {{ source('cars_src', 'carbodies_raw') }}
  
    