with
sales_data as (
    select * from {{ ref('int__sum_sales') }}
),

final as (
    select 
        s.location_code,
        s.city,
        s.state,
        s.ship_mode,
        s.total_sales
    from sales_data as s
    where s.segment = 'Consumer'
)

select * from final