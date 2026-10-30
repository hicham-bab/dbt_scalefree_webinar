{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__marketing__raw_campaign_attributions
parent_hashkey: hk_attribution_h
src_hashdiff: hd_attribution_marketing_n_s
src_payload:
    - attributed_revenue_eur
    - attribution_model
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
