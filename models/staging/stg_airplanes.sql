{{ config(materialized='view') }}

SELECT 
    airplane_id,
    airplane_model,
    manufacturer,
    -- This loaded_at field will be created during the ingestion process in Airflow
    loaded_at
FROM {{ source('s3_bolt_data', 'airplanes') }}
