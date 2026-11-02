-- One row per consumer loan. Book issued 2007-06 to 2011-12, observed 2016-05.
-- Identity columns never reach this model (see stg__lending__loan and int_loans).

with loans as (

    select * from {{ ref('int_loans') }}

)

select
    loan_id,
    issue_date,
    cast(year(issue_date) as integer)                               as issue_year,
    term_months,
    grade,
    sub_grade,
    purpose,
    home_ownership,
    verification_status,
    emp_length,
    addr_state,
    annual_income,
    debt_to_income_ratio,
    interest_rate_pct,
    loan_amount,
    funded_amount,
    installment,
    loan_status,
    -- Only matured loans have a final outcome. 'Current' loans (1,140) are still
    -- paying and must be left out of any charge-off rate denominator.
    loan_status in ('Fully Paid', 'Charged Off')                    as is_matured,
    loan_status = 'Charged Off'                                     as is_charged_off,
    earliest_credit_line_date,
    cast(datediff(month, earliest_credit_line_date, issue_date) / 12 as number(5, 1)) as credit_history_years_at_issue,
    delinquencies_2yrs,
    inquiries_last_6mths,
    open_accounts,
    public_records,
    public_record_bankruptcies,
    revolving_balance,
    revolving_utilization_pct,
    total_accounts,
    outstanding_principal,
    total_payment,
    total_received_principal,
    total_received_interest,
    total_received_late_fees,
    recoveries,
    collection_recovery_fee,
    -- Principal written off net of what collections brought back.
    cast(
        case
            when loan_status = 'Charged Off'
                then funded_amount - total_received_principal - (recoveries - collection_recovery_fee)
            else 0
        end
    as number(14, 2))                                               as net_principal_loss,
    last_payment_date

from loans