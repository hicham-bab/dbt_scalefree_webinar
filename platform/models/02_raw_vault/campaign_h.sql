{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__marketing__raw_campaigns
      bk_columns:
          - campaign_id
hashkey: hk_campaign_h
business_keys:
    - campaign_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
