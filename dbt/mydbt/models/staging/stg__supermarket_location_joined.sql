with 

supermarket_data as (
    select * from {{ ref('base__supermarket') }}
),

location_data as (
    select * from {{ ref('base__location') }}
),

final as (
    select
        sm.supermarket_id,
        sm.segment,
        sm.ship_mode,
        loc.city,
        loc.state,
        loc.postal_code as location_code,
        loc.country_code
    from supermarket_data as sm
    join location_data as loc
    on sm.location_code = loc.postal_code
)

select * from final