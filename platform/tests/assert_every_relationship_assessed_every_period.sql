-- 2,948 relationships x 36 periods: each relationship has exactly one assessment per period.
select relationship_id, count(*) as periods
from {{ ref('fct_risk_assessments') }}
group by 1
having count(*) <> (select count(*) from {{ ref('int_assessment_periods') }})
    or count(distinct period_number) <> count(*)