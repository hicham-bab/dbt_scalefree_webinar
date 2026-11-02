{{ config(
    materialized='incremental',
    tags=['link', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models: stg__oms__raw_product_reviews
link_hashkey: hk_product_review__customer__product_l
foreign_hashkeys:
    - hk_product_review_h
    - hk_customer_h
    - hk_product_h
{%- endset -%}

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
