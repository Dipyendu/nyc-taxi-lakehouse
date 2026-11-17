select payment_type, payment_type_name from {{ ref('taxi_payment_type') }}
