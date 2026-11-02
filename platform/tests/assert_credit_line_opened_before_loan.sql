-- Guards the two-digit-year fix in int_loans: no credit line can open after the loan.
select loan_id, earliest_credit_line_date, issue_date
from {{ ref('fct_loans') }}
where earliest_credit_line_date > issue_date