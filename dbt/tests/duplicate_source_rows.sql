-- Warning, not error: months with exact duplicate source rows (all source columns equal).
-- The trip key is the row position, so duplicates would pass the unique tests; this makes them visible.
{{ config(severity='warn') }}
select file_month, count(*) as duplicate_groups
from (
    select file_month
    from {{ source('bronze', 'yellow_trips') }}
    {% if var('month', none) %}where file_month = '{{ var("month") }}'{% endif %}
    group by file_month, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
        RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax,
        tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee,
        cbd_congestion_fee, request_source
    having count(*) > 1
)
group by file_month
