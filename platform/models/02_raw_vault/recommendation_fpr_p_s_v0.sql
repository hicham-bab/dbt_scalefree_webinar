{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__recommendations__fpr_records
parent_hashkey: hk_recommendation_h
src_hashdiff: hd_recommendation_fpr_p_s
src_payload:
    - customer_name
    - customer_email
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}