{{ config(materialized='view') }}

SELECT 
    customer_group_id,
    type,
    name,
    registry_number,
    -- This loaded_at field will be created during the ingestion process in Airflow
    loaded_at
FROM {{ source('s3_bolt_data', 'customer_groups') }}
