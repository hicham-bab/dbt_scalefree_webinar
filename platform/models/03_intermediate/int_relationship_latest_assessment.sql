-- The most recent assessment of each relationship (period 36, 2024-11-16).

select
    customer_id || '|' || institution_id   as relationship_id,
    assessment_date                        as latest_assessment_date,
    risk_score                             as latest_risk_score,
    risk_level                             as latest_risk_level,
    approval_recommendation                as latest_approval_recommendation

from {{ ref('int_risk_assessments') }}
qualify row_number() over (
    partition by customer_id, institution_id
    order by assessment_date desc
) = 1