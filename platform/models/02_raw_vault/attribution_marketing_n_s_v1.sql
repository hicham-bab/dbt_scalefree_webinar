{{ config(
    materialized='view',
    tags=['satellite', 'v1', 'raw_vault']
) }}

{%- set yaml_metadata -%}
sat_v0: attribution_marketing_n_s_v0
hashkey: hk_attribution_h
hashdiff: hd_attribution_marketing_n_s
add_is_current_flag: true
{%- endset -%}

{{ datavault4dbt.sat_v1(yaml_metadata=yaml_metadata) }}
