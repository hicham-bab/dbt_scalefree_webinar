{{ config(materialized='view', tags=['vault_staging', 'oms']) }}

{%- set yaml_metadata -%}
source_model: "raw_product_reviews"
ldts: '_loaded_at'
rsrc: '!OMS.raw_product_reviews'
hashed_columns:
    hk_product_review_h:
        - review_id
    hk_customer_h:
        - customer_id
    hk_product_h:
        - product_id
    hk_product_review__customer__product_l:
        - review_id
        - customer_id
        - product_id
    hd_product_review_oms_n_s:
        is_hashdiff: true
        columns:
            - rating
            - review_title
            - review_text
            - review_date
            - is_verified_purchase
            - helpful_votes
            - review_source
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
