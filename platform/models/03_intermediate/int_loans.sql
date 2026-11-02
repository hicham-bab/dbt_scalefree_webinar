-- Current version of each loan with the business rules applied. The raw vault keeps
-- the source's text-typed values ('10.65%', ' 36 months', 'Dec-11', 'NA'); they are
-- parsed here. Identity columns never entered the vault (see stg__lending__loan).

with current_s as (
    select *
    from {{ ref('loan_lending_n_s_v1') }}
    where is_current = true
)

select
    cast(h.loan_id as integer)                              as loan_id,
    cast(s.member_id as integer)                            as member_id,
    cast(s.loan_amnt as number(12, 2))                      as loan_amount,
    cast(s.funded_amnt as number(12, 2))                    as funded_amount,
    cast(s.funded_amnt_inv as number(12, 2))                as funded_amount_investors,
    -- ' 36 months' / ' 60 months'
    cast(trim(replace(s.term, 'months', '')) as integer)    as term_months,
    -- '10.65%'
    try_cast(replace(s.int_rate, '%', '') as number(6, 2))  as interest_rate_pct,
    cast(s.installment as number(10, 2))                    as installment,
    s.grade,
    s.sub_grade,
    s.emp_length,
    s.home_ownership,
    cast(s.annual_inc as number(14, 2))                     as annual_income,
    s.verification_status,
    -- 'Dec-11'
    to_date(s.issue_d, 'MON-YY')                            as issue_date,
    s.loan_status,
    s.purpose,
    s.zip_code                                              as zip3,
    s.addr_state,
    cast(s.dti as number(6, 2))                             as debt_to_income_ratio,
    cast(s.delinq_2yrs as integer)                          as delinquencies_2yrs,
    -- 'Nov-69' parses as 2069 with Snowflake's default century window, which puts
    -- 127 credit lines in the future. Any date after the loan's issue date is
    -- moved back one century.
    case
        when to_date(s.earliest_cr_line, 'MON-YY') > to_date(s.issue_d, 'MON-YY')
            then dateadd(year, -100, to_date(s.earliest_cr_line, 'MON-YY'))
        else to_date(s.earliest_cr_line, 'MON-YY')
    end                                                     as earliest_credit_line_date,
    cast(s.inq_last_6mths as integer)                       as inquiries_last_6mths,
    cast(s.open_acc as integer)                             as open_accounts,
    cast(s.pub_rec as integer)                              as public_records,
    cast(s.revol_bal as number(14, 2))                      as revolving_balance,
    try_cast(replace(s.revol_util, '%', '') as number(6, 2)) as revolving_utilization_pct,
    cast(s.total_acc as integer)                            as total_accounts,
    -- VARCHAR with the sentinel 'NA' on 697 loans
    try_cast(nullif(s.pub_rec_bankruptcies, 'NA') as integer) as public_record_bankruptcies,
    cast(s.out_prncp as number(12, 2))                      as outstanding_principal,
    cast(s.total_pymnt as number(14, 2))                    as total_payment,
    cast(s.total_rec_prncp as number(14, 2))                as total_received_principal,
    cast(s.total_rec_int as number(14, 2))                  as total_received_interest,
    cast(s.total_rec_late_fee as number(12, 2))             as total_received_late_fees,
    cast(s.recoveries as number(12, 2))                     as recoveries,
    cast(s.collection_recovery_fee as number(12, 2))        as collection_recovery_fee,
    case when nullif(s.last_pymnt_d, '') is not null then to_date(s.last_pymnt_d, 'MON-YY') end as last_payment_date,
    cast(s.last_pymnt_amnt as number(12, 2))                as last_payment_amount,
    s.ldts                                                  as synced_at

from {{ ref('loan_h') }} h
inner join current_s s on h.hk_loan_h = s.hk_loan_h
where h.hk_loan_h not in ({{ unknown_key() }}, {{ error_key() }})
  and not s.is_deleted