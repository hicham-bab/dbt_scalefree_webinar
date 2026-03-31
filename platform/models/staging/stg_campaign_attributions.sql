with source as (
    select * from {{ source('ecommerce_raw', 'raw_campaign_attributions') }}
),

renamed as (
    select
        cast(attribution_id as varchar(36))                as attribution_id,
        cast(customer_id as varchar(36))                   as customer_id,
        cast(campaign_id as varchar(36))                   as campaign_id,
        cast(order_id as varchar(36))                      as order_id,
        cast(first_touch_campaign_id as varchar(36))       as first_touch_campaign_id,
        cast(last_touch_campaign_id as varchar(36))        as last_touch_campaign_id,
        cast(attributed_revenue_eur as number(10,2))       as attributed_revenue_eur,
        cast(attribution_model as varchar(50))             as attribution_model,
        cast(_loaded_at as timestamp_ntz)                  as loaded_at
    from source
)

select * from renamed
