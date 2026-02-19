{{
  config(
    materialized = 'table',
    )
}}
WITH auction AS (
    SELECT * FROM {{ ref('stg_cars__auction_cars_raw') }}
),
stats AS (
    SELECT * FROM {{ ref('stg_cars__cars_raw') }}
),
enriched_with_closest AS (
    SELECT
        a.id_car,
        a.id_brand,
        a.id_model,
        a.id_carbody,
        a.year_releASe,
        a.mileage AS auction_mileage,
        a.transmission,
        a.drive_type,
        a.fuel_type,
        a.source_lot_id,
        a.link_source,
        a.auction_date,
        a.rate,
        a.equipment,
        s."cost" AS closest_mileage_price,
        s.mileage AS stat_mileage,
        s.year_releASe AS stat_year,
        -- Основной порядок: сначала разница в пробеге, потом год выпуска
        row_number() OVER (
            PARTITION BY a.id_car
            ORDER BY
                abs(a.mileage - s.mileage),
                abs(a.year_releASe - s.year_releASe)
        ) AS rn
    FROM auction a
    INNER JOIN stats s
        on a.id_brand    = s.id_brand
        and a.id_model   = s.id_model
        and a.id_carbody = s.id_carbody
        and a.rate       = s.rate
),
closest_match AS (
    SELECT *
    FROM enriched_with_closest
    where rn = 1
),

brands AS (SELECT id_brand, brand FROM {{ ref('stg_cars__brands_raw') }}),
models AS (SELECT id_model, model FROM {{ ref('stg_cars__models_raw') }}),
carbodies AS (SELECT id_carbody, carbody FROM {{ ref('stg_cars__carbodies_raw') }})

SELECT
    cm.id_car AS id,
    b.brand AS brand,
    m.model AS model,
    cb.carbody AS carbody,
    cm.year_releASe,
    cm.auction_mileage AS mileage,
    cm.closest_mileage_price AS price,
    cm.source_lot_id,
    cm.link_source,
    cm.auction_date,
    cm.rate,
    cm.equipment
FROM closest_match cm
LEFT JOIN brands b      ON cm.id_brand = b.id_brand
LEFT JOIN models m      ON cm.id_model = m.id_model
LEFT JOIN carbodies cb  ON cm.id_carbody = cb.id_carbody
where cm.year_releASe BETWEEN '2016' AND '2020'