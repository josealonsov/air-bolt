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
    
    -- Time calculations. All of these will then help us determine if some days of the week or hours of the day are more popular for demand planning/pricing
    (unix_timestamp(end_timestamp) - unix_timestamp(start_timestamp)) / 3600 as flight_duration_hours, 
    DATE(start_timestamp) as flight_date,
    HOUR(start_timestamp) as hour_of_day,
    DATE_FORMAT(start_timestamp, 'EEEE') as day_of_week,
    
    CASE 
        WHEN DAYOFWEEK(start_timestamp) IN (1, 7) THEN TRUE 
        ELSE FALSE 
    END as is_weekend,
    
    CASE 
        WHEN HOUR(start_timestamp) BETWEEN 1 AND 5 THEN 'Dawn'
        WHEN HOUR(start_timestamp) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN HOUR(start_timestamp) BETWEEN 12 AND 18 THEN 'Afternoon'
        WHEN HOUR(start_timestamp) BETWEEN 19 AND 24 THEN 'Night'
    END as time_of_day_category,
    
    CONCAT(origin_city, ' - ', destination_city) as route,

    CURRENT_TIMESTAMP() as updated_at

FROM {{ ref('stg_trips') }}
{% if is_incremental() %}
    -- Only process new trips or trips that havent been completed
    where loaded_at > (select max(updated_at) from {{ this }}) OR DATE(COALESCE(end_timestamp,'2050-01-01')) >= CURRENT_DATE()
{% endif %}