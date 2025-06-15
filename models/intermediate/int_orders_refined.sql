{{ 
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    ) 
}}

SELECT 
    o.order_id,
    o.customer_id,
    o.trip_id,
    DATE(t.start_timestam) as flight_date
    o.price,
    o.seat_number_assigned,
    o.status,

    -- flags
    CASE WHEN status = 'Booked' THEN TRUE ELSE FALSE END as is_active,
    CASE WHEN status = 'Finished' THEN TRUE ELSE FALSE END as is_completed,
    CASE WHEN status = 'Cancelled' THEN TRUE ELSE FALSE END as is_cancelled,
    
    SUBSTRING(seat_number_assigned, 0, LENGTH(seat_number_assigned) - 1) as seat_row,
    SUBSTRING(seat_number_assigned, -1, 1) as seat_position,

    CURRENT_TIMESTAMP() as updated_at

FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_trips') }} t ON o.trip_id = t.trip_id
{% if is_incremental() %}
    where o.loaded_at > (select max(updated_at) from {{ this }})
{% endif %}