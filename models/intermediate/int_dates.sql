-- models/marts/dim_date.sql
{{ config(materialized='table') }}

WITH dates AS (
    SELECT 
        date_add('2024-08-01', id) as date --assuming start date on august 1, 2024 given dataset provided
    FROM range(CAST(DATEDIFF(CURRENT_DATE(), '2024-08-01')AS INT ))  -- provides all dates since start of operations until current date
)
    SELECT 
        date,
        YEAR(date_key) as year,
        QUARTER(date_key) as quarter,
        MONTH(date_key) as month,
        WEEK(date_key) as week,
        DAYOFMONTH(date_key) as day,
        DAYOFWEEK(date_key) as day_of_week,
        DATE_FORMAT(date_key, 'MMMM') as month_name,
        DATE_FORMAT(date_key, 'EEEE') as day_name,
        DAYOFWEEK(date_key) IN (1, 7) as is_weekend,
        CONCAT('Q', quarter(date_key)) as quarter_name
    FROM dates
