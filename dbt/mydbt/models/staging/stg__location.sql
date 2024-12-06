with
source as (
    select postal_code, city, state, region, country from {{ source('raw_retail_supermarket', 'location') }}
)
, final as (
    select
        postal_code,
        city,
        state,
        region,
        country
    from source
)

select * from final