{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__risk_platform__risk_assess_monthly_assessments
      bk_columns:
          - assessment_id
hashkey: hk_assessment_h
business_keys:
    - assessment_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}