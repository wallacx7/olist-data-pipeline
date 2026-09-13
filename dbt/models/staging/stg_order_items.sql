with source as (

    select * from {{ source('olist_raw', 'order_items') }}

),

renamed as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,

        -- Valor total do item (produto + frete), útil para agregações no fato
        price + freight_value as total_item_value

    from source

)

select * from renamed