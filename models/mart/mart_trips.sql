{{ 
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge'
    ) 
}}

SELECT 
    trip_id,
    origin_city,
    destination_city,
    airplane_id,
    start_timestamp,
    end_timestamp,
    
    flight_duration_hours, 
    flight_date,
    hour_of_day as flight_hour_of_day,
    day_of_week as flight_day_of_week,
    
    is_weekend as is_weekend_flight,
    
    time_of_day_category,
    
    route,

    CURRENT_TIMESTAMP() as updated_at

FROM {{ ref('int_trips') }}
{% if is_incremental() %}
    -- Only process trips that havent been completed
    where DATE(COALESCE(end_timestamp,'2050-01-01')) >= CURRENT_DATE()
{% endif %}