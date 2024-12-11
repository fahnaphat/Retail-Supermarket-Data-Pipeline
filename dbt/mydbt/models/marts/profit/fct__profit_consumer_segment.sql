with
profit_data as (
    select * from {{ ref('int__sum_profit') }}
),

final as (
    select 
        pd.location_code,
        pd.city,
        pd.state,
        pd.ship_mode,
        pd.total_profit
    from profit_data as pd
    where pd.segment = 'Consumer'
)

select * from final