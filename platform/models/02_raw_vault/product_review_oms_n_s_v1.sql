{{ config(
    materialized='view',
    tags=['satellite', 'v1', 'raw_vault']
) }}

{%- set yaml_metadata -%}
sat_v0: product_review_oms_n_s_v0
hashkey: hk_product_review_h
hashdiff: hd_product_review_oms_n_s
ledts_alias: load_end_date
add_is_current_flag: true
{%- endset -%}

{{ datavault4dbt.sat_v1(yaml_metadata=yaml_metadata) }}
