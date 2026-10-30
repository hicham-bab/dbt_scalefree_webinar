{{ config(
    materialized='view',
    tags=['satellite', 'v1', 'raw_vault']
) }}

{%- set yaml_metadata -%}
sat_v0: customer_crm_p_s_v0
hashkey: hk_customer_h
hashdiff: hd_customer_crm_p_s
add_is_current_flag: true
{%- endset -%}

{{ datavault4dbt.sat_v1(yaml_metadata=yaml_metadata) }}
