{{ config(materialized='view', tags=['vault_staging', 'pim']) }}

{%- set yaml_metadata -%}
source_model: "raw_products"
ldts: '_loaded_at'
rsrc: '!PIM.raw_products'
hashed_columns:
    hk_product_h:
        - product_id
    hd_product_pim_n_s:
        is_hashdiff: true
        columns:
            - product_name
            - category
            - subcategory
            - material
            - color
            - unit_price
            - cost_price
            - sku_code
            - collection
            - is_active
            - launched_at
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
