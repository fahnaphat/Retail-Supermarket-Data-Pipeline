with 

goods_data as (
    select * from {{ ref('base__goods') }}
),

category_data as (
    select * from {{ ref('base__category') }}
),

final as (
    select 
        g.id as goods_id,
        c.name as category,
        g.name as goods_name,
        g.sales,
        g.quantity,
        g.discount,
        g.profit,
        g.supermarket_location_code
    from goods_data as g
    join category_data as c
    on g.category_id = c.id
)
    
select * from final