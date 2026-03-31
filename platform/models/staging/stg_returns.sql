with source as (
    select * from {{ source('ecommerce_raw', 'raw_returns') }}
),

renamed as (
    select
        cast(return_id as varchar(36))                     as return_id,
        cast(order_id as varchar(36))                      as order_id,
        nullif(cast(order_item_id as varchar(36)), '')     as order_item_id,
        cast(return_reason as varchar(100))                as return_reason,
        cast(return_requested_at as date)                  as return_requested_at,
        cast(return_received_at as date)                   as return_received_at,
        cast(refund_amount as number(10,2))                as refund_amount,
        cast(refund_status as varchar(30))                 as refund_status,
        cast(_loaded_at as timestamp_ntz)                  as loaded_at
    from source
)

select * from renamed
