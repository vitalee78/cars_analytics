/*Правила именования структур*/
-- https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview

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
  
    