{{ config(
    materialized='view',
    tags=['satellite', 'v1', 'raw_vault']
) }}

{%- set yaml_metadata -%}
sat_v0: assessment_customer_institution_nl_s_v0
hashkey: hk_assessment_customer_institution_nl
hashdiff: hd_assessment_customer_institution_nl_s
add_is_current_flag: true
{%- endset -%}

{{ datavault4dbt.sat_v1(yaml_metadata=yaml_metadata) }}