with

supermarket_location_data as (
    select * from {{ ref('stg__supermarket_location_joined') }}
),

goods_category_data as (
    select * from {{ ref('stg__goods_category_joined') }}
),

final as (
    select
        sl.supermarket_id,
        sl.location_code,
        sl.segment,
        sl.ship_mode,
        sl.city,
        sl.state,
        sl.country_code,
        gc.goods_name,
        gc.category,
        gc.sales,
        gc.quantity,
        gc.discount,
        gc.profit
    from supermarket_location_data as sl
    join goods_category_data as gc
    on sl.location_code = gc.supermarket_location_code
)

select * from final