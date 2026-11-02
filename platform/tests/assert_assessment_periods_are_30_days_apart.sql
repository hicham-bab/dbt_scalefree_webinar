-- The risk platform assesses on a fixed 30-day cadence. If this ever fails, the
-- "periods are not calendar months" caveat in context/ needs revisiting.
select *
from {{ ref('int_assessment_periods') }}
where days_since_previous_period is not null
  and days_since_previous_period <> 30