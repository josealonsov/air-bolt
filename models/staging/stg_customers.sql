{{ config(materialized='view') }}

SELECT 
    customer_id,
    name,
    customer_group_id,
    email,
    phone_number,
    -- This loaded_at field will be created during the ingestion process in Airflow
    loaded_at
FROM {{ source('s3_bolt_data', 'customers') }}
