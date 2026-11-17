-- One row per clean silver trip; codes normalised for the dimensions.
{{ config(materialized='view') }}
select
    trip_id,
    file_month,
    vendor_id,
    pickup_ts,
    dropoff_ts,
    cast(pickup_ts as date) as pickup_date,
    hour(pickup_ts) as pickup_hour,
    pu_location_id,
    do_location_id,
    coalesce(ratecode_id, 99) as ratecode_id,  -- dictionary: 99 = Null/unknown
    payment_type,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    (unix_timestamp(dropoff_ts) - unix_timestamp(pickup_ts)) / 60.0 as duration_min,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount,
    request_source
from {{ source('silver', 'yellow_trips') }}
