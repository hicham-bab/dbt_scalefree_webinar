with source as (
    select * from {{ source('ecommerce_raw', 'raw_payments') }}
),

renamed as (
    select
        cast(payment_id as varchar(36))                    as payment_id,
        cast(order_id as varchar(36))                      as order_id,
        cast(payment_date as date)                         as payment_date,
        cast(payment_method as varchar(50))                as payment_method,
        cast(amount as number(10,2))                       as amount,
        cast(currency as varchar(10))                      as currency,
        -- Normalize to EUR using approximate rates
        round(
            cast(amount as number(10,2)) *
            case upper(cast(currency as varchar(10)))
                when 'EUR' then 1.0
                when 'GBP' then 1.17
                when 'USD' then 0.92
                when 'CHF' then 1.03
                else 1.0
            end,
            2
        )                                                  as amount_eur,
        cast(payment_status as varchar(30))                as payment_status,
        cast(gateway as varchar(50))                       as gateway,
        cast(_loaded_at as timestamp_ntz)                  as loaded_at
    from source
)

select * from renamed
