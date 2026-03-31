with source as (
    select * from {{ source('ecommerce_raw', 'raw_web_events') }}
),

renamed as (
    select
        cast(event_id as varchar(36))                      as event_id,
        cast(session_id as varchar(36))                    as session_id,
        nullif(cast(customer_id as varchar(36)), '')       as customer_id,
        cast(event_type as varchar(50))                    as event_type,
        cast(event_timestamp as timestamp_ntz)             as event_timestamp,
        cast(date_trunc('day', cast(event_timestamp as timestamp_ntz)) as date) as event_date,
        cast(page_url as varchar(500))                     as page_url,
        nullif(cast(product_id as varchar(36)), '')        as product_id,
        cast(device_type as varchar(20))                   as device_type,
        cast(browser as varchar(50))                       as browser,
        nullif(cast(utm_source as varchar(100)), '')       as utm_source,
        case
            when nullif(cast(customer_id as varchar(36)), '') is not null
            then true
            else false
        end                                                as is_authenticated,
        cast(_loaded_at as timestamp_ntz)                  as loaded_at
    from source
)

select * from renamed
