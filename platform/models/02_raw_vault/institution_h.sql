{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__risk_platform__risk_assess_financial_institutions
      bk_columns:
          - institution_id
hashkey: hk_institution_h
business_keys:
    - institution_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}