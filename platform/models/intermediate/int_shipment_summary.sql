-- Summarizes shipment data at order level, computing delivery performance metrics.
-- Grain: one row per order.
with shipments as (
    select * from {{ ref('stg_shipments') }}
),

orders as (
    select order_id, order_date from {{ ref('stg_orders') }}
),

-- In case there are multiple shipments per order, take the primary one
shipment_ranked as (
    select
        s.*,
        row_number() over (partition by s.order_id order by s.shipped_at asc) as rn
    from shipments s
),

primary_shipment as (
    select * from shipment_ranked where rn = 1
),

final as (
    select
        ps.order_id,
        ps.shipment_id,
        ps.carrier,
        ps.tracking_number,
        ps.shipment_status,
        ps.shipped_at,
        ps.delivered_at,
        ps.estimated_delivery_date,
        datediff('day', o.order_date, ps.shipped_at)                            as days_to_ship,
        case
            when ps.delivered_at is not null
            then datediff('day', ps.shipped_at, ps.delivered_at)
            else null
        end                                                                      as days_to_deliver,
        case
            when ps.delivered_at is not null
            then datediff('day', o.order_date, ps.delivered_at)
            else null
        end                                                                      as total_fulfillment_days,
        case
            when ps.delivered_at is not null
                and datediff('day', ps.shipped_at, ps.delivered_at) > 7
            then true
            else false
        end                                                                      as is_late_delivery
    from primary_shipment ps
    left join orders o on ps.order_id = o.order_id
)

select * from final
