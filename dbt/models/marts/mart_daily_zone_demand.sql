-- Daily demand by pickup zone. Amounts are gross: trips quarantined for a negative fare or total
-- (refunds, disputes and others) are not in the fact table, so nothing is subtracted for them.
select
    concat(cast(f.pickup_date as string), '-', cast(f.pu_location_id as string)) as demand_key,
    f.pickup_date,
    f.pu_location_id,
    z.borough,
    z.zone,
    count(*) as trips,
    round(sum(f.total_amount), 2) as gross_total_amount,
    round(avg(f.trip_distance), 3) as avg_trip_distance
from {{ ref('fct_trips') }} f
join {{ ref('dim_zone') }} z on z.location_id = f.pu_location_id
group by all
