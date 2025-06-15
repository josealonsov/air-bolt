{{ config(materialized='view') }}

SELECT 
    trip_id,
    origin_city,
    destination_city,
    airplane_id,
    start_timestamp,
    end_timestamp,
    -- This loaded_at field will be created during the ingestion process in Airflow
    loaded_at
FROM {{ source('s3_bolt_data', 'trips') }}
