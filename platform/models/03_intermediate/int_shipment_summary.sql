{# Summarizes shipment data at order level, computing delivery performance metrics.
   Grain: one row per order. #}

with current_shipment_sat as (
    select *
    from {{ ref('shipment_logistics_n_s_v1') }}
    where is_current = true
),

shipments as (
    select
        sh.shipment_id,
        o.order_id,
        s.carrier,
        s.tracking_number,
        s.estimated_delivery_date,
        s.shipped_at,
        s.delivered_at,
        s.shipment_status,
        sh.ldts                         as loaded_at
    from {{ ref('shipment_h') }} sh
    join {{ ref('shipment_order_nl') }} l  on sh.hk_shipment_h = l.hk_shipment_h
    join {{ ref('order_h') }} o            on l.hk_order_h     = o.hk_order_h
    join current_shipment_sat s            on sh.hk_shipment_h = s.hk_shipment_h
    where sh.hk_shipment_h not in ({{ unknown_key() }}, {{ error_key() }})
),

current_order_sat as (
    select *
    from {{ ref('order_oms_n_s_v1') }}
    where is_current = true
),

orders as (
    select o.order_id, s.order_date
    from {{ ref('order_h') }} o
    join current_order_sat s on o.hk_order_h = s.hk_order_h
    where o.hk_order_h not in ({{ unknown_key() }}, {{ error_key() }})
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
