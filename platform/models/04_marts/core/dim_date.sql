-- Calendar spine for the Semantic Layer. Covers the loan book (2007-2016), the risk
-- platform (2022-2024) and the recommendation log (2024-2025).

{{ config(materialized = 'table') }}

with days as (

    select dateadd(day, seq4(), cast('2007-01-01' as date)) as date_day
    from table(generator(rowcount => 7305))

)

select
    date_day,
    cast(year(date_day) as integer)                         as year_number,
    cast(quarter(date_day) as integer)                      as quarter_number,
    cast(month(date_day) as integer)                        as month_number,
    cast(date_trunc('month', date_day) as date)             as month_start_date,
    cast(dayofweekiso(date_day) as integer)                 as iso_day_of_week,
    dayofweekiso(date_day) in (6, 7)                        as is_weekend
from days