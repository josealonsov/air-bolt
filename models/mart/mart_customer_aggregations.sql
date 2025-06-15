{{ config(materialized='table') }}

SELECT
    c.customer_id,
    c.name,
    c.customer_group_id,
    c.email,
    c.phone_number,
    c.total_bookings,
    c.total_spent,
    c.avg_booking_value,
    c.first_flight_date,
    c.last_flight_date,
    c.days_since_last_booking,
    c.customer_tier,
    c.is_frequent_flyer,
    c.cancellation_rate,

    CURRENT_TIMESTAMP() as updated_at
FROM {{ ref('int_customer_aggregations') }} c
