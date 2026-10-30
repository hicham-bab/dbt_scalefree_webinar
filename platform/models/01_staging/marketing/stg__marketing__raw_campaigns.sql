{{ config(materialized='view', tags=['vault_staging', 'marketing']) }}

{%- set yaml_metadata -%}
source_model: "raw_campaigns"
ldts: '_loaded_at'
rsrc: '!Marketing.raw_campaigns'
hashed_columns:
    hk_campaign_h:
        - campaign_id
    hd_campaign_marketing_n_s:
        is_hashdiff: true
        columns:
            - campaign_name
            - campaign_type
            - channel
            - start_date
            - end_date
            - budget_eur
            - target_segment
            - utm_source
            - utm_medium
            - utm_campaign
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
