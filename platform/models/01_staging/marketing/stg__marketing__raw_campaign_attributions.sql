{{ config(materialized='view', tags=['vault_staging', 'marketing']) }}

{%- set yaml_metadata -%}
source_model: "raw_campaign_attributions"
ldts: '_loaded_at'
rsrc: '!Marketing.raw_campaign_attributions'
hashed_columns:
    hk_attribution_h:
        - attribution_id
    hk_customer_h:
        - customer_id
    hk_order_h:
        - order_id
    hk_campaign_attributed_h:
        - campaign_id
    hk_campaign_first_touch_h:
        - first_touch_campaign_id
    hk_campaign_last_touch_h:
        - last_touch_campaign_id
    hk_attribution_customer_order_campaign_l:
        - attribution_id
        - customer_id
        - order_id
        - campaign_id
        - first_touch_campaign_id
        - last_touch_campaign_id
    hd_attribution_marketing_n_s:
        is_hashdiff: true
        columns:
            - attributed_revenue_eur
            - attribution_model
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
