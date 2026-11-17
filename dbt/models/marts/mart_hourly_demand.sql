-- Demand by month and hour of pickup. Amounts are gross (see mart_daily_zone_demand).
select
    concat(file_month, '-', lpad(cast(pickup_hour as string), 2, '0')) as demand_key,
    file_month,
    pickup_hour,
    count(*) as trips,
    round(avg(total_amount), 2) as avg_gross_total_amount,
    round(avg(duration_min), 2) as avg_duration_min
from {{ ref('fct_trips') }}
group by all
