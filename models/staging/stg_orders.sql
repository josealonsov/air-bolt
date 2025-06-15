{{ config(materialized='view') }}

SELECT 
    order_id,
    customer_id,
    trip_id,
    price,
    seat_number_assigned,
    status,
    -- This loaded_at field will be created during the ingestion process in Airflow
    loaded_at
FROM {{ source('s3_bolt_data', 'orders') }}
