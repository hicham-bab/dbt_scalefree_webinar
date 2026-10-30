{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__marketing__raw_campaign_attributions
      bk_columns:
          - attribution_id
hashkey: hk_attribution_h
business_keys:
    - attribution_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
