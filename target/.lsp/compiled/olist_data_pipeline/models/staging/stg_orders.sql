with source as (

    select * from `olist-data-pipeline-503121`.`olist_raw`.`orders`

),

renamed as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,

        -- Métrica derivada útil para análise de performance de entrega
        date_diff(
            date(order_delivered_customer_date),
            date(order_purchase_timestamp),
            day
        ) as delivery_days,

        -- Flag para identificar pedidos entregues com atraso
        case
            when order_delivered_customer_date > order_estimated_delivery_date
            then true
            else false
        end as is_late_delivery

    from source

)

select * from renamed