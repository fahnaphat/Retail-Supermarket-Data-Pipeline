{{ config( materialized='table' ) }}

with
supermarket_data as (
    select * from {{ ref('stg__supermarket') }}
),

final_sum_sales as (
    select 
        s.supermarket_id,
        s.location_code,
        s.city,
        s.state,
        s.segment,
        s.ship_mode,
        CAST(SUM(s.sales) AS NUMERIC(10,2)) as total_sales
    from supermarket_data as s
    group by
        s.supermarket_id,
        s.location_code, 
        s.city, 
        s.state, 
        s.segment,
        s.ship_mode
)

select * from final_sum_sales order by supermarket_id