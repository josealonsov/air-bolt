{{ config(materialized='table') }}
-- to make this model efficiently incremental, rolling calculations would have to be made on a separate model because they reference last 365 days of trips data

WITH date_base AS (
    SELECT 
            date as aggregation_date,
            year,
            quarter,
            month,
            week,
            day,
            day_of_week,
            month_name,
            day_name,
            is_weekend

    FROM {{ ref('int_dates') }}
),

flight_metrics AS (
    SELECT 
        t.flight_date as aggregation_date,
        COUNT(t.trip_id) as total_flights,
        COUNT(distinct t.airplane_id) as unique_airplanes_used,
        COUNT(distinct t.route) as unique_routes,
        AVG(t.flight_duration_hours) as avg_flight_duration_hours,
        SUM(case when t.is_weekend then 1 else 0 end) as weekend_flights,
        SUM(case when t.time_of_day_category = 'Morning' then 1 else 0 end) as morning_flights,
        SUM(case when t.time_of_day_category = 'Afternoon' then 1 else 0 end) as afternoon_flights,
        SUM(case when t.time_of_day_category = 'Night' then 1 else 0 end) as night_flights,
        SUM(case when t.time_of_day_category = 'Dawn' then 1 else 0 end) as dawn_flights,

        avg(a.max_seats) as avg_airplane_capacity_used,
        SUM(case when a.manufacturer = 'Boeing' then 1 else 0 end) as boeing_airplane_count,
        SUM(case when a.manufacturer = 'Airbus' then 1 else 0 end) as airbus_airplane_count,
        SUM(case when a.manufacturer = 'Embraer' then 1 else 0 end) as embraer_airplane_count,
        SUM(case when a.manufacturer = 'Bombardier' then 1 else 0 end) as bombardier_airplane_count,
        SUM(case when a.manufacturer = 'Gulfstream' then 1 else 0 end) as gulfstream_airplane_count,

        SUM(case when a.airplane_size_category = 'Very Large' then 1 else 0 end) as very_large_airplane_flights,
        SUM(case when a.airplane_size_category = 'Large' then 1 else 0 end) as large_airplane_flights,
        SUM(case when a.airplane_size_category = 'Medium' then 1 else 0 end) as medium_airplane_flights,
        SUM(case when a.airplane_size_category = 'Small' then 1 else 0 end) as small_airplane_flights
    FROM {{ ref('int_trips') }} t
    LEFT JOIN {{ ref('int_airplane_details') }} a ON t.airplane_id = a.airplane_id
    GROUP BY flight_date
),

booking_metrics AS (
    SELECT 
        flight_date as aggregation_date,
        COUNT(distinct customer_id) as customers_who_booked,
        SUM(price) as total_revenue,
        SUM(price) / NULLIFZERO(count(distinct o.trip_id), 0) as avg_revenue_per_trip,
        SUM(case when is_active then price else 0 end) as completed_revenue,
        SUM(case when is_completed then price else 0 end) as completed_revenue,
        SUM(case when is_cancelled then price else 0 end) as cancelled_revenue,
        AVG(price) as avg_booking_price,
        MIN(price) as min_booking_price,
        MAX(price) as max_booking_price,
        SUM(case when is_active then 1 else 0 end) as orders_active,
        SUM(case when is_completed then 1 else 0 end) as orders_completed,
        SUM(case when is_cancelled then 1 else 0 end) as orders_cancelled
    FROM {{ ref('int_orders_refined') }}
    GROUP BY flight_date
)
-- Final aggregated daily metrics

SELECT 
    d.aggregation_date,
    
    COALESCE(fm.total_flights, 0) as total_flights,
    COALESCE(fm.unique_airplanes_used, 0) as unique_airplanes_used,
    COALESCE(fm.unique_routes, 0) as unique_routes,
    COALESCE(fm.avg_flight_duration_hours, NULL) as avg_flight_duration_hours,
    COALESCE(fm.weekend_flights, 0) as weekend_flights,
    COALESCE(fm.morning_flights, 0) as morning_flights,
    COALESCE(fm.afternoon_flights, 0) as afternoon_flights,
    COALESCE(fm.night_flights, 0) as night_flights,
    COALESCE(fm.dawn_flights, 0) as dawn_flights,
    
    COALESCE(fm.avg_airplane_capacity_used, NULL) as avg_airplane_capacity_used,

    COALESCE(fm.boeing_airplane_count, 0) as boeing_airplane_count,
    COALESCE(fm.airbus_airplane_count, 0) as airbus_airplane_count,
    COALESCE(fm.embraer_airplane_count, 0) as embraer_airplane_count,
    COALESCE(fm.bombardier_airplane_count, 0) as bombardier_airplane_count,
    COALESCE(fm.gulfstream_airplane_count, 0) as gulfstream_airplane_count,
    
    COALESCE(fm.very_large_airplane_flights, 0) as very_large_airplane_flights,
    COALESCE(fm.large_airplane_flights, 0) as large_airplane_flights,
    COALESCE(fm.medium_airplane_flights, 0) as medium_airplane_flights,
    COALESCE(fm.small_airplane_flights, 0) as small_airplane_flights,
    
    COALESCE(bm.customers_who_booked, 0) as customers_who_booked,
    COALESCE(bm.total_revenue, 0) as total_revenue,
    COALESCE(bm.avg_revenue_per_trip, NULL) as avg_revenue_per_trip,
    COALESCE(bm.completed_revenue, 0) as completed_revenue,
    COALESCE(bm.cancelled_revenue, 0) as cancelled_revenue,
    COALESCE(bm.avg_booking_price, NULL) as avg_booking_price,
    COALESCE(bm.min_booking_price, NULL) as min_booking_price,
    COALESCE(bm.max_booking_price, NULL) as max_booking_price,

    -- rolling revenue calculations
    SUM(total_revenue) OVER (ORDER BY d.aggregation_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS last_5days_revenue,
    SUM(total_revenue) OVER (ORDER BY d.aggregation_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last_7days_revenue,
    SUM(total_revenue) OVER (ORDER BY d.aggregation_date ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS last_15days_revenue,
    SUM(total_revenue) OVER (ORDER BY d.aggregation_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS last_30days_revenue,
    SUM(total_revenue) OVER (ORDER BY d.aggregation_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS last_60days_revenue,
    SUM(total_revenue) OVER (ORDER BY d.aggregation_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS last_90days_revenue,
    SUM(total_revenue) OVER (ORDER BY d.aggregation_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) AS last_180days_revenue,
    SUM(total_revenue) OVER (ORDER BY d.aggregation_date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS last_365days_revenue,
    
    COALESCE(bm.orders_active, 0) as orders_active,
    COALESCE(bm.orders_completed, 0) as orders_completed,
    COALESCE(bm.orders_cancelled, 0) as orders_cancelled,
    
    CASE 
        WHEN COALESCE(bm.orders_active, 0) + COALESCE(bm.orders_completed, 0) + COALESCE(bm.orders_cancelled, 0) > 0 
        THEN COALESCE(bm.orders_completed, 0) / (COALESCE(bm.orders_active, 0) + COALESCE(bm.orders_completed, 0) + COALESCE(bm.orders_cancelled, 0))
        ELSE 0 
    END as completion_rate,
    
    CASE 
        WHEN COALESCE(bm.orders_active, 0) + COALESCE(bm.orders_completed, 0) + COALESCE(bm.orders_cancelled, 0) > 0 
        THEN COALESCE(bm.orders_cancelled, 0) / (COALESCE(bm.orders_active, 0) + COALESCE(bm.orders_completed, 0) + COALESCE(bm.orders_cancelled, 0))
        ELSE 0 
    END as cancellation_rate,
    
    CASE 
        WHEN COALESCE(fm.total_flights, 0) > 0 
        THEN COALESCE(bm.customers_who_booked, 0) / fm.total_flights
        ELSE NULL
    END as avg_customers_per_flight
    
    CURRENT_TIMESTAMP() as updated_at

FROM date_base d
LEFT JOIN flight_metrics fm ON d.aggregation_date = fm.aggregation_date
LEFT JOIN booking_metrics bm ON d.aggregation_date = bm.aggregation_date
ORDER BY d.aggregation_date
