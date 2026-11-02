{{ config(
    materialized='incremental',
    tags=['link', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__risk_platform__risk_assess_risk_profiles
      link_hashkey: hk_customer_institution_l
      foreign_hashkeys:
          - hk_customer_h
          - hk_institution_h
    - name: stg__risk_platform__risk_assess_performance_metrics
      link_hashkey: hk_customer_institution_l
      foreign_hashkeys:
          - hk_customer_h
          - hk_institution_h
link_hashkey: hk_customer_institution_l
foreign_hashkeys:
    - hk_customer_h
    - hk_institution_h
{%- endset -%}

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}