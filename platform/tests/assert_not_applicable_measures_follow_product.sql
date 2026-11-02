-- Blank collateral / liquidity / cash-flow values mean "not applicable to this
-- product", never "missing". A NULL on an applicable product, or a value on a
-- non-applicable one, breaks that contract.
select relationship_id, product_type, collateral_quality_score, liquidity_ratio, projected_cash_flow_rating
from {{ ref('dim_credit_relationships') }}
where (collateral_quality_score is null) = (product_type in ('Mortgage', 'Business Loan'))
   or (liquidity_ratio is null) = (product_type = 'Business Loan')
   or (projected_cash_flow_rating is null) = (product_type in ('Business Loan', 'Personal Loan'))