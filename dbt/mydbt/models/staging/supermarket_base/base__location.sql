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
        case
            when country = 'United States' then 'US'
            else country
        end as country_code
    from source
)

select * from final