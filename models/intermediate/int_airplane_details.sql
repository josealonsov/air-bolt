{{ config(materialized='table') }}

SELECT 
    a.airplane_id,
    a.airplane_model,
    a.manufacturer,
    am.max_seats,
    am.max_weight,
    am.max_distance,
    am.engine_type,
    
    CASE 
        WHEN am.max_seats <= 50 THEN 'Small'
        WHEN am.max_seats <= 100 THEN 'Medium'
        WHEN am.max_seats <= 200 THEN 'Large'
        WHEN am.max_seats <= 400 THEN 'Very Large'
    END as airplane_size_category,
    
    CASE 
        WHEN am.max_distance <= 2500 THEN 'Short-haul'
        WHEN am.max_distance <= 5000 THEN 'Medium-haul'
        WHEN am.max_distance <= 10000 THEN 'Long-haul'
        WHEN am.max_distance > 10000 THEN 'Ultra-long-haul'
    END as airplane_range_category

FROM {{ ref('stg_airplanes') }} a
LEFT JOIN {{ ref('stg_airplane_models') }} am
    ON a.manufacturer = am.manufacturer 
    AND a.airplane_model = am.airplane_model
