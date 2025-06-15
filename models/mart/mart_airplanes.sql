{{ config(materialized='table') }}

SELECT 
    airplane_id,
    airplane_model,
    manufacturer,
    max_seats,
    max_weight,
    max_distance,
    engine_type,
    aircraft_size_category,
    aircraft_range_category,

    CURRENT_TIMESTAMP() as updated_at
FROM {{ ref('int_airplane_details') }}
