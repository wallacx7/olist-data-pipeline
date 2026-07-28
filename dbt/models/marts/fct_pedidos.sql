with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

final as (

    select
        -- Chave composta: grão = item do pedido
        oi.order_id,
        oi.order_item_id,

        -- Chaves estrangeiras para as dimensões
        oi.product_id,
        oi.seller_id,
        o.customer_id,
        date(o.order_purchase_timestamp) as order_date,

        -- Atributos degenerados (vêm do fato, sem dimensão própria)
        o.order_status,
        o.delivery_days,
        o.is_late_delivery,

        -- Medidas aditivas (podem ser somadas com segurança neste grão)
        oi.price,
        oi.freight_value,
        oi.total_item_value

    from order_items oi
    left join orders o
        on oi.order_id = o.order_id

)

select * from final