{# Gold warehouse dimension. One row per warehouse, sourced from the warehouse hub
   and its current descriptive satellite. #}

with current_warehouse as (
    select *
    from {{ ref('warehouse_inventory_n_s_v1') }}
    where is_current = true
),

final as (
    select
        h.warehouse_id,
        w.warehouse_name,
        h.ldts                          as loaded_at
    from {{ ref('warehouse_h') }} h
    left join current_warehouse w on h.hk_warehouse_h = w.hk_warehouse_h
    where h.hk_warehouse_h not in ({{ unknown_key() }}, {{ error_key() }})
)

select * from final
