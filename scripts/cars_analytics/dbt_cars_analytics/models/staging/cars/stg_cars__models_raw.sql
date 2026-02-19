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