with
source as (
    select * from {{ source('raw_retail_supermarket', 'supermarket') }}
)
, final as (
    select
        id as supermarket_id,
        segment,
        ship_mode,
        location_code
    from source
)

select * from final