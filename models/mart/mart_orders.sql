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
    o.flight_date as flight_date_key,
    o.price,
    o.seat_number_assigned,
    o.status,
    o.seat_row,
    o.seat_position,
    o.is_completed,
    o.is_cancelled,

    CURRENT_TIMESTAMP() as updated_at

FROM {{ ref('int_orders_refined') }} o

{% if is_incremental() %}
--updating data for active/recently completed orders/flights
    WHERE o.flight_date >= DATE_SUB(CURRENT_DATE(), 2)
{% endif %}
