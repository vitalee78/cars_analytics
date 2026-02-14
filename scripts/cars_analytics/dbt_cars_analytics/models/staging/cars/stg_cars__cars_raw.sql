/*Правила именования структур*/
-- https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview

{{
    config(
      materialized = 'table'
      )
}}

SELECT
  id_car, id_brand, id_model, id_carbody, "cost", year_release, rate, mileage, 
  transmission, drive_type, fuel_type, source_lot_id, link_source, lot_date, 
  equipment, created_at, updated_at, id
FROM {{ source('cars_src', 'cars_raw') }}