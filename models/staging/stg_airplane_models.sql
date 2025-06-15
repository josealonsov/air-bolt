{{ config(materialized='view') }}

SELECT 
    manufacturer,
    airplane_model,
    max_seats,
    max_weight,
    max_distance,
    engine_type,
    -- This loaded_at field will be created during the ingestion process in Airflow
    loaded_at
FROM {{ source('s3_bolt_data', 'airplane_models_flattened') }}
