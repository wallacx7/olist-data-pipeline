with source as (

    select * from {{ source('olist_raw', 'product_category_translation') }}

),

renamed as (

    select
        -- A carga raw não capturou os nomes de coluna corretamente
        -- (autodetect do BigQuery falhou nesse CSV específico, de só 2
        -- colunas, e usou nomes genéricos). Corrigido aqui na staging,
        -- que é exatamente seu papel: padronizar o que vem malformado.
        string_field_0 as product_category_name,
        string_field_1 as product_category_name_english

    from source

)

select * from renamed