/*Правила именования структур*/
-- https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview

{{
    config(
      materialized = 'table'
      )
}}

SELECT
  id_car, id_brand, id_model, id_carbody, year_release, mileage, 
  transmission, drive_type, fuel_type, start_price, final_price, source_lot_id, 
  link_source, auction_date, rate, equipment, created_at, updated_at, id
FROM {{ source('cars_src', 'auction_cars_raw') }}