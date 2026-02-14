/*Правила именования структур*/
-- https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview

{{
      config(
        materialized = 'table'
        )
}}

SELECT 
    id_model, 
    id_brand, 
    model, 
    created_at
FROM
    {{ source('cars_src', 'models_raw') }}