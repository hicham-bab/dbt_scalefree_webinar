-- The governed approval_rate uses the source system's definition. If the source
-- ever changes what approval_percentage counts, this fails before a dashboard drifts.
with recomputed as (
    select relationship_id, 100 * avg(iff(is_approved, 1, 0)) as approval_pct
    from {{ ref('fct_risk_assessments') }}
    group by 1
)
select r.relationship_id, r.source_approval_pct, recomputed.approval_pct
from {{ ref('dim_credit_relationships') }} as r
inner join recomputed on r.relationship_id = recomputed.relationship_id
where abs(r.source_approval_pct - recomputed.approval_pct) > 0.1