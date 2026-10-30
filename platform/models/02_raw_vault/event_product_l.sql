{{ config(
    materialized='incremental',
    tags=['link', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__web_analytics__raw_web_events
      link_hashkey: hk_event_product_l
      foreign_hashkeys:
          - hk_event_h
          - hk_product_h
link_hashkey: hk_event_product_l
foreign_hashkeys:
    - hk_event_h
    - hk_product_h
{%- endset -%}

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
