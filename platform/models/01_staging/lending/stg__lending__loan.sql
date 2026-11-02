{{ config(materialized='view', tags=['vault_staging', 'lending']) }}

{#
  Identity columns (ssn, social_security_number, ssnumber, ssnumber1, dl,
  drivers_license) and free text that can identify a borrower (emp_title, c_url,
  c_desc, title) are passed through this view from the landing table but are NOT
  in any hashdiff or satellite, so dbt never persists them. In production, block
  or hash them in the Fivetran connector schema settings so they never land at all.
#}

{%- set yaml_metadata -%}
source_model:
    financial_services: 'loan'
ldts: '_fivetran_synced'
rsrc: '!LENDING.loan'
derived_columns:
    loan_id:
        value: 'id'
        datatype: 'NUMBER(38,0)'
    is_deleted:
        value: 'coalesce(_fivetran_deleted, false)'
        datatype: 'BOOLEAN'
hashed_columns:
    hk_loan_h:
        - loan_id
    hd_loan_lending_n_s:
        is_hashdiff: true
        columns:
            - member_id
            - loan_amnt
            - funded_amnt
            - funded_amnt_inv
            - term
            - int_rate
            - installment
            - grade
            - sub_grade
            - emp_length
            - home_ownership
            - annual_inc
            - verification_status
            - issue_d
            - loan_status
            - purpose
            - zip_code
            - addr_state
            - dti
            - delinq_2yrs
            - earliest_cr_line
            - inq_last_6mths
            - open_acc
            - pub_rec
            - revol_bal
            - revol_util
            - total_acc
            - pub_rec_bankruptcies
            - out_prncp
            - total_pymnt
            - total_rec_prncp
            - total_rec_int
            - total_rec_late_fee
            - recoveries
            - collection_recovery_fee
            - last_pymnt_d
            - last_pymnt_amnt
            - is_deleted
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}