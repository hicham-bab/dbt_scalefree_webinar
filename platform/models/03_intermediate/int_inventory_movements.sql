{# Reconstructs inventory movements at event grain from the raw vault.
   Grain: one row per inventory movement (movement_id).
   Relationships to product / warehouse are resolved via the descriptive business
   keys carried on the movement satellite (the movement_product_warehouse_l link is
   the canonical DV relationship and can be substituted once link loads are wired). #}

with current_movement as (
    select *
    from {{ ref('inventory_movement_inventory_n_s_v1') }}
    where is_current = true
),

movements as (
    select
        h.movement_id,
        s.product_id,
        s.warehouse_id,
        s.movement_type,
        s.reason_code,
        s.quantity,
        s.unit_cost,
        s.occurred_at,
        h.ldts                          as loaded_at
    from {{ ref('inventory_movement_h') }} h
    join current_movement s on h.hk_inventory_movement_h = s.hk_inventory_movement_h
    where h.hk_inventory_movement_h not in ({{ unknown_key() }}, {{ error_key() }})
),

final as (
    select
        m.movement_id,
        m.product_id,
        m.warehouse_id,
        m.movement_type,
        m.reason_code,
        m.quantity,
        m.unit_cost,
        m.occurred_at,
        cast(m.occurred_at as date)                             as movement_date,
        -- Direction is implied by movement_type; quantity is always positive at source.
        case m.movement_type
            when 'RECEIPT'        then  m.quantity
            when 'RETURN_RESTOCK' then  m.quantity
            when 'SHIPMENT'       then -m.quantity
            when 'ADJUSTMENT'     then -m.quantity
            else 0
        end                                                     as signed_quantity,
        round(m.quantity * m.unit_cost, 2)                      as movement_value_eur,
        m.loaded_at
    from movements m
)

select * from final
