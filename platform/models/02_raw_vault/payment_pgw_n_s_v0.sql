{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__payment_gateway__raw_payments
parent_hashkey: hk_payment_h
src_hashdiff: hd_payment_pgw_n_s
src_payload:
    - payment_date
    - payment_method
    - amount
    - currency
    - payment_status
    - gateway
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
