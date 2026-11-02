{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__risk_platform__risk_assess_customers
parent_hashkey: hk_customer_h
src_hashdiff: hd_customer_risk_platform_p_s
src_payload:
    - customer_name
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}