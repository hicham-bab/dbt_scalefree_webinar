with current_order_sat as (
    select *
    from {{ ref('order_oms_n_s_v1') }}
    where is_current = true
),

orders as (
    select
        o.order_id,
        c.customer_id,
        s.order_date,
        s.order_status,
        s.channel,
        s.shipping_country,
        s.shipping_method,
        s.discount_code,
        coalesce(s.discount_amount, 0)  as discount_amount,
        o.ldts                          as loaded_at
    from {{ ref('order_h') }} o
    join {{ ref('order_customer_l') }} l   on o.hk_order_h   = l.hk_order_h
    join {{ ref('customer_h') }} c         on l.hk_customer_h = c.hk_customer_h
    join current_order_sat s               on o.hk_order_h   = s.hk_order_h
    where o.hk_order_h not in ({{ unknown_key() }}, {{ error_key() }})
),

current_product_sat as (
    select *
    from {{ ref('product_pim_n_s_v1') }}
    where is_current = true
),

order_items as (
    select
        oi.order_item_id,
        o.order_id,
        p.product_id,
        s.quantity,
        round(
            s.quantity
            * s.unit_price_at_order
            * (1 - coalesce(s.discount_pct, 0) / 100),
            2
        )                               as line_revenue
    from {{ ref('order_item_order_product_nl') }} nl
    join {{ ref('order_item_order_product_nl_s_v1') }} s
        on nl.hk_order_item_order_product_nl = s.hk_order_item_order_product_nl
        and s.is_current = true
    join {{ ref('order_item_h') }} oi  on nl.hk_order_item_h = oi.hk_order_item_h
    join {{ ref('order_h') }} o        on nl.hk_order_h      = o.hk_order_h
    join {{ ref('product_h') }} p      on nl.hk_product_h    = p.hk_product_h
    where oi.hk_order_item_h not in ({{ unknown_key() }}, {{ error_key() }})
),

products as (
    select h.product_id, s.category
    from {{ ref('product_h') }} h
    join current_product_sat s on h.hk_product_h = s.hk_product_h
    where h.hk_product_h not in ({{ unknown_key() }}, {{ error_key() }})
),

returns as (
    select o.order_id
    from {{ ref('return_h') }} r
    join {{ ref('return_order_order_item_l') }} l on r.hk_return_h = l.hk_return_h
    join {{ ref('order_h') }} o                   on l.hk_order_h  = o.hk_order_h
    where r.hk_return_h not in ({{ unknown_key() }}, {{ error_key() }})
    group by o.order_id
),

-- Aggregate items to order level
item_agg as (
    select
        oi.order_id,
        count(oi.order_item_id)                                  as item_count,
        sum(oi.quantity)                                         as total_quantity,
        sum(oi.line_revenue)                                     as gross_revenue,
        listagg(distinct p.category, ', ')
            within group (order by p.category)                  as product_categories
    from order_items oi
    left join products p on oi.product_id = p.product_id
    group by oi.order_id
),

-- Determine first order per customer
first_orders as (
    select
        customer_id,
        min(order_date) as first_order_date
    from orders
    group by customer_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.channel,
        o.order_status,
        o.shipping_country,
        o.shipping_method,
        o.discount_code,
        o.discount_amount,
        coalesce(ia.item_count, 0)                               as item_count,
        coalesce(ia.total_quantity, 0)                           as total_quantity,
        coalesce(ia.gross_revenue, 0)                            as gross_revenue,
        o.discount_amount                                        as discount_amount_hdr,
        coalesce(ia.gross_revenue, 0) - o.discount_amount       as net_revenue,
        ia.product_categories,
        case when r.order_id is not null then true else false end as has_returned,
        case when o.order_date = fo.first_order_date then true else false end as is_first_order
    from orders o
    left join item_agg ia on o.order_id = ia.order_id
    left join returns r on o.order_id = r.order_id
    left join first_orders fo on o.customer_id = fo.customer_id
)

select * from final
