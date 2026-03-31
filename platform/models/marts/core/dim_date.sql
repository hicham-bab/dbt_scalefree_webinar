-- Date dimension spanning 2022-01-01 to 2026-12-31.
-- Uses Snowflake-native generator function. No external packages required.
with date_spine as (
    select dateadd('day', seq4(), cast('2022-01-01' as date)) as date_day
    from table(generator(rowcount => 1826))
),

final as (
    select
        date_day,
        cast(to_char(date_day, 'YYYYMMDD') as integer)             as date_key,
        dayofweek(date_day)                                         as day_of_week,
        dayname(date_day)                                           as day_name,
        month(date_day)                                             as month_num,
        monthname(date_day)                                         as month_name,
        quarter(date_day)                                           as quarter,
        year(date_day)                                              as year,
        case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend,
        case when dayofweek(date_day) not in (0, 6) then true else false end as is_weekday,
        weekofyear(date_day)                                        as week_of_year,
        dayofyear(date_day)                                         as day_of_year,
        case when day(date_day) = 1 then true else false end        as is_first_day_of_month,
        case
            when date_day = last_day(date_day) then true else false
        end                                                         as is_last_day_of_month,
        -- Fiscal quarter assuming April fiscal year start
        case
            when month(date_day) in (4, 5, 6)   then 'FQ1'
            when month(date_day) in (7, 8, 9)   then 'FQ2'
            when month(date_day) in (10, 11, 12) then 'FQ3'
            when month(date_day) in (1, 2, 3)   then 'FQ4'
        end                                                         as fiscal_quarter,
        case
            when month(date_day) in (4, 5, 6)   then year(date_day)
            when month(date_day) in (7, 8, 9)   then year(date_day)
            when month(date_day) in (10, 11, 12) then year(date_day)
            when month(date_day) in (1, 2, 3)   then year(date_day) - 1
        end                                                         as fiscal_year,
        -- Season (Northern Hemisphere)
        case
            when month(date_day) in (3, 4, 5)   then 'Spring'
            when month(date_day) in (6, 7, 8)   then 'Summer'
            when month(date_day) in (9, 10, 11)  then 'Autumn'
            when month(date_day) in (12, 1, 2)  then 'Winter'
        end                                                         as season,
        to_char(date_day, 'YYYY-MM')                                as year_month,
        to_char(date_day, 'YYYY')                                   as year_str
    from date_spine
)

select * from final
