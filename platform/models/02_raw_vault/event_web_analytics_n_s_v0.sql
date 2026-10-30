{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__web_analytics__raw_web_events
parent_hashkey: hk_event_h
src_hashdiff: hd_event_web_analytics_n_s
src_payload:
    - session_id
    - event_type
    - event_timestamp
    - page_url
    - device_type
    - browser
    - utm_source
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
