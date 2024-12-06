with
source as (
    select * from {{ source('raw_retail_supermarket', 'goods')}}
),
final as (
    select
        id,
        category_id,
        supermarket_code as supermarket_location_code,
        name,
        sales,
        quantity,
        discount,
        profit
    from source
)

select * from final