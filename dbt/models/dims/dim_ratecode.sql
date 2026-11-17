select ratecode_id, ratecode_name from {{ ref('taxi_ratecode') }}
