{{ config(
    materialized='incremental',
    tags=['non_historized_link', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__risk_platform__risk_assess_monthly_assessments
      link_hashkey: hk_assessment_customer_institution_nl
      foreign_hashkeys:
          - hk_assessment_h
          - hk_customer_h
          - hk_institution_h
link_hashkey: hk_assessment_customer_institution_nl
foreign_hashkeys:
    - hk_assessment_h
    - hk_customer_h
    - hk_institution_h
{%- endset -%}

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}