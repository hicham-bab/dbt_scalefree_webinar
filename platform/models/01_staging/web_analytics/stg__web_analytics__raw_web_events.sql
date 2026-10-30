{{ config(materialized='view', tags=['vault_staging', 'web_analytics']) }}

{%- set yaml_metadata -%}
source_model: "raw_web_events"
ldts: '_loaded_at'
rsrc: '!Web_Analytics.raw_web_events'
hashed_columns:
    hk_event_h:
        - event_id
    hk_customer_h:
        - customer_id
    hk_product_h:
        - product_id
    hk_event_customer_l:
        - event_id
        - customer_id
    hk_event_product_l:
        - event_id
        - product_id
    hd_event_web_analytics_n_s:
        is_hashdiff: true
        columns:
            - session_id
            - event_type
            - event_timestamp
            - page_url
            - device_type
            - browser
            - utm_source
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
