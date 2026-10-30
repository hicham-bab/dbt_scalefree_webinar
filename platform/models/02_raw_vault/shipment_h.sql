{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__logistics__raw_shipments
      bk_columns:
          - shipment_id
hashkey: hk_shipment_h
business_keys:
    - shipment_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
