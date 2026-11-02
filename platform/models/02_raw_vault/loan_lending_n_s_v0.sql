{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__lending__loan
parent_hashkey: hk_loan_h
src_hashdiff: hd_loan_lending_n_s
src_payload:
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

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}