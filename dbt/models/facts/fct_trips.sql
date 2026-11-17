-- Incremental fact. Every incremental run needs --vars '{month: YYYY-MM}' and replaces exactly that
-- month (idempotent reload). The first build of an empty table loads every month in silver.
{% if is_incremental() and not var('month', none) %}
  {{ exceptions.raise_compiler_error("fct_trips: pass --vars '{month: YYYY-MM}'; an incremental run replaces one month") }}
{% endif %}
{{ config(
    materialized='incremental',
    incremental_strategy='replace_where',
    incremental_predicates=(["file_month = '" ~ var('month') ~ "'"] if var('month', none) else none),
    partition_by=['file_month'],
    on_schema_change='append_new_columns'
) }}
select
    trip_id,
    file_month,
    pickup_date,
    pickup_hour,
    pu_location_id,
    do_location_id,
    vendor_id,
    ratecode_id,
    payment_type,
    passenger_count,
    trip_distance,
    duration_min,
    fare_amount,
    tip_amount,
    tolls_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount
from {{ ref('stg_trips') }}
{% if is_incremental() and var('month', none) %}where file_month = '{{ var("month") }}'{% endif %}
