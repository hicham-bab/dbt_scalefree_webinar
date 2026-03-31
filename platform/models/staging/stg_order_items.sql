with source as (
    select * from {{ source('ecommerce_raw', 'raw_order_items') }}
),

renamed as (
    select
        cast(order_item_id as varchar(36))                   as order_item_id,
        cast(order_id as varchar(36))                        as order_id,
        cast(product_id as varchar(36))                      as product_id,
        cast(quantity as number(5,0))                        as quantity,
        cast(unit_price_at_order as number(10,2))            as unit_price_at_order,
        coalesce(cast(discount_pct as number(5,2)), 0)       as discount_pct,
        -- Compute line revenue: qty * price * (1 - discount%)
        round(
            cast(quantity as number(5,0))
            * cast(unit_price_at_order as number(10,2))
            * (1 - coalesce(cast(discount_pct as number(5,2)), 0) / 100),
            2
        )                                                    as line_revenue,
        cast(_loaded_at as timestamp_ntz)                    as loaded_at
    from source
)

select * from renamed
