{{ 
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    ) 
}}

WITH customer_order_metrics AS (
    SELECT 
        customer_id,
        COUNT(*) as total_bookings,
        SUM(price) as total_spent,
        AVG(price) as avg_booking_value,
        MIN(flight_date) as first_flight_date,
        MAX(flight_date) as last_flight_date,
        SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END) as cancelled_bookings,
        SUM(CASE WHEN is_completed THEN 1 ELSE 0 END) as completed_bookings
    FROM {{ ref('int_orders_refined') }}
    {% if is_incremental() %}
        -- only recalculate metrics for customers with new orders since last update
        where customer_id in (
            select distinct customer_id 
            from {{ ref('int_orders_refined') }}
            where updated_at >= (select MAX(updated_at) from {{ this }})
        )
    {% endif %}
    GROUP BY customer_id
),

customer_tiers AS (
    SELECT 
        customer_id,
        CAST( MONTHS_BETWEEN( CURRENT_DATE(), first_flight_date ) AS INT ) as months_since_first_flight,
        CASE 
            WHEN total_bookings = 0 THEN 'Inactive'
            WHEN total_bookings/NULLIFZERO(months_since_first_booking) <= 0.1 THEN 'Ocasional'
            WHEN total_bookings/NULLIFZERO(months_since_first_booking) <= 0.2 THEN 'Regular'
            WHEN total_bookings/NULLIFZERO(months_since_first_booking) <= 0.5 THEN 'Frequent'
            WHEN total_bookings/NULLIFZERO(months_since_first_booking) <= 1 THEN 'VIP'
            WHEN total_bookings/NULLIFZERO(months_since_first_booking) > 1 THEN 'Gold'
            ELSE NULL
        END as customer_tier,

        CASE WHEN customer_tier in ('Frequent','VIP','Gold') THEN TRUE ELSE FALSE END as is_frequent_flyer,

        cancelled_bookings/total_bookings as cancellation_rate
    FROM customer_order_metrics
)

SELECT 
    c.customer_id,
    c.name,
    c.customer_group_id,
    c.email,
    c.phone_number,
    
    -- aggregated metrics
    COALESCE(com.total_bookings, 0) as total_bookings,
    COALESCE(com.total_spent, 0) as total_spent,
    COALESCE(com.avg_booking_value, 0) as avg_booking_value,
    com.first_flight_date,
    com.last_flight_date,
    DATEDIFF(CURRENT_DATE(), com.last_flight_date) as days_since_last_booking,
    ct.customer_tier as customer_tier,
    COALESCE(ct.is_frequent_flyer, FALSE) as is_frequent_flyer,
    COALESCE(ct.cancellation_rate, 0) as cancellation_rate,
    
    CURRENT_TIMESTAMP() as updated_at

FROM {{ ref('stg_customers') }} c
LEFT JOIN customer_order_metrics com ON c.customer_id = com.customer_id
LEFT JOIN customer_tiers ct ON c.customer_id = ct.customer_id

{% if is_incremental() %}
    --limiting the customer universe to only those with new orders since last update, then merging
    where c.customer_id in (
        select distinct customer_id 
        from {{ ref('int_orders_refined') }}
        where updated_at > (select MAX(updated_at) from {{ this }}) 
    )
    OR
    --or new registered customers without orders
    c.loaded_at >= (select MAX(updated_at) from {{ this }})
{% endif %}