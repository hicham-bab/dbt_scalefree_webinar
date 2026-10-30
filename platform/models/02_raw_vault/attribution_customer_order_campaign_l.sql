{{ config(
    materialized='incremental',
    tags=['link', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__marketing__raw_campaign_attributions
      link_hashkey: hk_attribution_customer_order_campaign_l
      foreign_hashkeys:
          - hk_attribution_h
          - hk_customer_h
          - hk_order_h
          - hk_campaign_attributed_h
          - hk_campaign_first_touch_h
          - hk_campaign_last_touch_h
link_hashkey: hk_attribution_customer_order_campaign_l
foreign_hashkeys:
    - hk_attribution_h
    - hk_customer_h
    - hk_order_h
    - hk_campaign_attributed_h
    - hk_campaign_first_touch_h
    - hk_campaign_last_touch_h
{%- endset -%}

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
