{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__web_analytics__raw_web_events
      bk_columns:
          - event_id
hashkey: hk_event_h
business_keys:
    - event_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
