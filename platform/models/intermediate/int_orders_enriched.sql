{#
  ============================================================================
  INTERMEDIATE MODELS
  ============================================================================
  Intermediate models sit between staging and marts. They join, aggregate,
  or reshape cleaned data WITHOUT adding business definitions. Think of them
  as reusable building blocks for mart models.

  CONFIG EXPLAINED:

  - materialized = 'table'
      We use 'table' because this model aggregates many rows (order_items ->
      order level) and is referenced by multiple downstream models. A table
      avoids re-computing the aggregation every time.

  - schema = 'intermediate'
      Routes this model to the <target_schema>_intermediate schema.

  - tags = ['intermediate', 'orders']
      Allows selective runs: dbt run --select tag:intermediate

  - cluster_by = ['order_date', 'customer_id']
      On warehouses like Snowflake or Databricks, clustering organizes the
      data on disk by these columns. Queries that filter on order_date or
      customer_id will scan less data = faster + cheaper.
  ============================================================================
#}

{{
  config(
    materialized = 'table',
    schema = 'intermediate',
    tags = ['intermediate', 'orders'],
    cluster_by = ['order_date', 'customer_id']
  )
}}

-- ============================================================================
-- INTERMEDIATE MODEL: int_orders_enriched
-- ============================================================================
-- Purpose : Join orders with line items, products, and returns to produce
--           order-level metrics (revenue, item count, flags).
-- Grain   : One row per order
-- Depends : stg_orders, stg_order_items, stg_products, stg_returns
-- ============================================================================

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select product_id, category from {{ ref('stg_products') }}
),

returns as (
    select order_id from {{ ref('stg_returns') }}
    group by order_id
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
