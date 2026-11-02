-- One row per assessment period. The risk platform assesses every relationship on
-- the same 36 dates, 30 days apart. period_number (1..36) is the honest time axis;
-- calendar months are not, because January and May 2022 each contain two periods
-- and February 2022 contains none.

with dates as (

    select distinct assessment_date
    from {{ ref('int_risk_assessments') }}

)

select
    assessment_date,
    cast(dense_rank() over (order by assessment_date) as integer)  as period_number,
    cast(datediff(day, lag(assessment_date) over (order by assessment_date), assessment_date) as integer) as days_since_previous_period
from dates