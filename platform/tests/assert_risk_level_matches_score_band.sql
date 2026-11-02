-- risk_level is a band of risk_score. Agents are told so; prove it. Bounds are
-- inclusive: 54 assessments score exactly 0.200 and are labelled Very Low.
select assessment_id, risk_score, risk_level
from {{ ref('fct_risk_assessments') }}
where not (
       (risk_level = 'Very Low'  and risk_score <= 0.2)
    or (risk_level = 'Low'       and risk_score >= 0.2 and risk_score <= 0.4)
    or (risk_level = 'Moderate'  and risk_score >= 0.4 and risk_score <= 0.6)
    or (risk_level = 'High'      and risk_score >= 0.6 and risk_score <= 0.8)
    or (risk_level = 'Very High' and risk_score >= 0.8)
)