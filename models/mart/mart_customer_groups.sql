{{ config(materialized='table') }}

SELECT 
    customer_group_id,
    type,
    name,
    registry_number,

    CURRENT_TIMESTAMP() as updated_at
FROM {{ ref('stg_customer_groups') }}
