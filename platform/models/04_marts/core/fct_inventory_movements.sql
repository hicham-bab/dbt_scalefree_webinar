{# Gold fact table: one row per inventory movement event.
   Supports stock-flow analysis (receipts vs shipments), net stock change,
   and inventory value moved, broken down by product, warehouse and time. #}

with movements as (
    select * from {{ ref('int_inventory_movements') }}
),

final as (
    select
        movement_id,
        product_id,
        warehouse_id,
        movement_date,
        cast(to_char(movement_date, 'YYYYMMDD') as integer)     as movement_date_key,
        movement_type,
        reason_code,
        quantity,
        signed_quantity,
        unit_cost,
        movement_value_eur,
        occurred_at
    from movements
)

select * from final
