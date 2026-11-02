{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__bank_marketing__predict_term_deposit
parent_hashkey: hk_contact_h
src_hashdiff: hd_contact_bank_marketing_n_s
src_payload:
    - age
    - job
    - marital
    - education
    - c_default
    - balance
    - housing
    - loan
    - contact
    - day
    - month
    - duration
    - campaign
    - pdays
    - previous
    - poutcome
    - y
    - is_deleted
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}