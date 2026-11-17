-- Calendar from the first data month to the end of next year, so new months always find their dates.
with days as (
    select explode(sequence(to_date('2024-01-01'), last_day(add_months(trunc(current_date(), 'YEAR'), 23)), interval 1 day)) as date_day
)
select
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    date_format(date_day, 'yyyy-MM') as year_month,
    dayofweek(date_day) as day_of_week,      -- 1 = Sunday
    date_format(date_day, 'EEEE') as day_name,
    dayofweek(date_day) in (1, 7) as is_weekend
from days
