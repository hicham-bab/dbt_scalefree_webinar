with source as (
    select * from {{ source('ecommerce_raw', 'raw_shipments') }}
),

renamed as (
    select
        cast(shipment_id as varchar(36))                   as shipment_id,
        cast(order_id as varchar(36))                      as order_id,
        cast(carrier as varchar(50))                       as carrier,
        cast(tracking_number as varchar(100))              as tracking_number,
        cast(estimated_delivery_date as date)              as estimated_delivery_date,
        cast(shipped_at as date)                           as shipped_at,
        nullif(cast(delivered_at as varchar(50)), '')      as delivered_at_raw,
        case
            when nullif(cast(delivered_at as varchar(50)), '') is not null
            then cast(delivered_at as date)
            else null
        end                                                as delivered_at,
        cast(shipment_status as varchar(30))               as shipment_status,
        cast(_loaded_at as timestamp_ntz)                  as loaded_at
    from source
)

select * from renamed
