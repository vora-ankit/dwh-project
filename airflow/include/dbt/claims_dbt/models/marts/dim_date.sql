with days as (
    select dateadd(day, row_number() over (order by seq4()) - 1, '2020-01-01'::date) as date_day
    from table(generator(rowcount => 3650))
)
select
    to_number(to_char(date_day, 'YYYYMMDD')) as date_key,
    date_day,
    year(date_day)        as year,
    quarter(date_day)     as quarter,
    month(date_day)       as month,
    monthname(date_day)   as month_name,
    dayname(date_day)     as day_name,
    dayofweek(date_day) in (0, 6) as is_weekend
from days