{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__oms__raw_product_reviews
parent_hashkey: hk_product_review_h
src_hashdiff: hd_product_review_oms_n_s
src_payload:
    - rating
    - review_title
    - review_text
    - review_date
    - is_verified_purchase
    - helpful_votes
    - review_source
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
