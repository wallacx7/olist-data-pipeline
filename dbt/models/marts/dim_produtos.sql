with products as (
    select * from {{ ref('stg_products') }}
),

category_translation as (
    select * from {{ ref('stg_product_category_translation') }}
),

final as (
    select
        p.product_id,
        p.product_category_name,
        -- fallback: se não houver tradução, mantém o nome original em PT
        coalesce(t.product_category_name_english, p.product_category_name) as product_category_name_english,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm

    from products p
    left join category_translation t
        on p.product_category_name = t.product_category_name
)

select * from final