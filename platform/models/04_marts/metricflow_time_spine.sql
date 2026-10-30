{{ config(materialized='table') }}

{#-
  MetricFlow time spine — one row per calendar day.
  Required by the dbt Semantic Layer for time-based metrics: cumulative metrics
  (e.g. revenue_last_7_days / revenue_last_30_days), time offsets, and querying any
  metric by `metric_time` at a given granularity.

  Range is static (2022-01-01 .. 2027-01-01, end exclusive) so it fully covers the
  demo data (dim_date spans 2022-01-01..2026-12-31) independent of the current date.
  Built with the core `dbt.date_spine` macro (no dbt_utils dependency).
-#}

with base_dates as (

    {{ dbt.date_spine(
        'day',
        "cast('2022-01-01' as date)",
        "cast('2027-01-01' as date)"
    ) }}

),

final as (

    select cast(date_day as date) as date_day
    from base_dates

)

select * from final
