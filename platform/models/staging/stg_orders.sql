with source as (
    select * from {{ source('ecommerce_raw', 'raw_orders') }}
),

renamed as (
    select
        cast(order_id as varchar(36))                      as order_id,
        cast(customer_id as varchar(36))                   as customer_id,
        cast(order_date as date)                           as order_date,
        cast(order_status as varchar(30))                  as order_status,
        cast(channel as varchar(50))                       as channel,
        cast(shipping_city as varchar(100))                as shipping_city,
        cast(shipping_country as varchar(100))             as shipping_country,
        cast(shipping_method as varchar(50))               as shipping_method,
        nullif(cast(discount_code as varchar(50)), '')     as discount_code,
        coalesce(cast(discount_amount as number(10,2)), 0) as discount_amount,
        cast(_loaded_at as timestamp_ntz)                  as loaded_at
    from source
)

select * from renamed
