with source as (
    select * from {{ source('ecommerce_raw', 'raw_campaigns') }}
),

renamed as (
    select
        cast(campaign_id as varchar(36))                   as campaign_id,
        cast(campaign_name as varchar(255))                as campaign_name,
        cast(campaign_type as varchar(50))                 as campaign_type,
        cast(channel as varchar(50))                       as channel,
        cast(start_date as date)                           as start_date,
        cast(end_date as date)                             as end_date,
        cast(budget_eur as number(12,2))                   as budget_eur,
        cast(target_segment as varchar(100))               as target_segment,
        cast(utm_source as varchar(100))                   as utm_source,
        cast(utm_medium as varchar(100))                   as utm_medium,
        cast(utm_campaign as varchar(100))                 as utm_campaign,
        datediff('day', cast(start_date as date), cast(end_date as date)) as campaign_duration_days,
        cast(_loaded_at as timestamp_ntz)                  as loaded_at
    from source
)

select * from renamed
