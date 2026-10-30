{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__marketing__raw_campaigns
parent_hashkey: hk_campaign_h
src_hashdiff: hd_campaign_marketing_n_s
src_payload:
    - campaign_name
    - campaign_type
    - channel
    - start_date
    - end_date
    - budget_eur
    - target_segment
    - utm_source
    - utm_medium
    - utm_campaign
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
