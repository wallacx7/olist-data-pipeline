with order_payments as (
    select * from {{ ref('stg_order_payments') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

final as (

    select
        -- Chave composta: grão = pagamento. Um pedido pode ter mais de uma
        -- linha aqui (ex: parte no cartão + parte em voucher), cada uma com
        -- seu próprio payment_sequential — esse é o grão nativo da fonte.
        op.order_id,
        op.payment_sequential,

        -- Atributos do pagamento
        op.payment_type,
        op.payment_installments,

        -- Atributo degenerado do pedido (sem passar pelo grão de item —
        -- é por isso que este fato existe separado de fct_pedidos: juntar
        -- pagamento [grão pedido] no fato de itens [grão item] duplicaria
        -- o valor pago em pedidos com mais de um item, um fan-out clássico)
        date(o.order_purchase_timestamp) as order_date,

        -- Medida aditiva
        op.payment_value

    from order_payments op
    left join orders o
        on op.order_id = o.order_id

)

select * from final
