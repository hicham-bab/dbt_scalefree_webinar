{{
  config(

    -- This is a FACT table: it records measurable business events (orders).
    -- Facts are typically the largest tables and benefit most from incremental
    -- materialization in production.

    -- INCREMENTAL: instead of rebuilding the entire table on each run,
    -- dbt only processes NEW or CHANGED rows. This is critical for large
    -- fact tables (millions of rows) where a full rebuild would be slow/costly.
    materialized = 'incremental',

    schema = 'core',

    tags = ['mart', 'core', 'orders'],

    -- UNIQUE_KEY: tells dbt which column(s) uniquely identify a row.
    -- During incremental runs, dbt uses this to MERGE (upsert):
    --   - New order_ids get INSERTed
    --   - Existing order_ids get UPDATEd (e.g., status changed to 'shipped')
    unique_key = 'order_id',

    -- INCREMENTAL_STRATEGY: how dbt handles the merge.
    --   'merge'           -> MERGE INTO (best for updates + inserts)
    --   'delete+insert'   -> DELETE matching rows, then INSERT (simpler)
    --   'insert_overwrite'-> replace entire partitions (good for date-partitioned)
    --   'append'          -> INSERT only, no updates (fastest, for immutable data)
    incremental_strategy = 'merge',

    -- ON_SCHEMA_CHANGE: what happens when you add/remove columns from the SQL.
    --   'ignore'   -> do nothing (new columns silently dropped)
    --   'append_new_columns' -> ALTER TABLE ADD COLUMN for new ones
    --   'sync_all_columns'   -> add new + drop removed columns
    --   'fail'     -> raise an error so you handle it explicitly
    on_schema_change = 'append_new_columns',

    grants = {'select': ['analyst_role', 'finance_role']},

    contract = {'enforced': true}
  )
}}

-- ============================================================================
-- FACT MODEL: fct_orders
-- ============================================================================
-- Purpose : One row per order with revenue, payment, and shipment details.
--           This is the central fact table for order analytics.
-- Grain   : One row per order_id
-- Depends : int_orders_enriched, int_payment_summary, int_shipment_summary
-- ============================================================================

with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

payments as (
    select * from {{ ref('int_payment_summary') }}
),

shipments as (
    select * from {{ ref('int_shipment_summary') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        cast(to_char(o.order_date, 'YYYYMMDD') as integer)                     as order_date_key,
        o.channel,
        o.order_status,
        o.shipping_country,
        o.shipping_method,
        o.discount_code,
        o.item_count,
        o.total_quantity,
        o.gross_revenue                                                         as gross_revenue_eur,
        o.discount_amount_hdr                                                   as discount_amount_eur,
        o.net_revenue                                                           as net_revenue_eur,
        o.product_categories,
        o.has_returned,
        o.is_first_order,
        -- Payment details
        coalesce(p.payment_method, 'unknown')                                   as payment_method,
        coalesce(p.payment_status, 'unknown')                                   as payment_status,
        p.total_paid_eur,
        p.is_refunded,
        -- Shipment details
        s.carrier,
        s.shipment_status,
        coalesce(s.days_to_ship, 0)                                             as days_to_ship,
        s.days_to_deliver,
        coalesce(s.is_late_delivery, false)                                     as is_late_delivery
    from orders o
    left join payments p on o.order_id = p.order_id
    left join shipments s on o.order_id = s.order_id
)

select * from final
