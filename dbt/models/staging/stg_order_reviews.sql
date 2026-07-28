with source as (

    select * from {{ source('olist_raw', 'order_reviews') }}

),

renamed as (

    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,

        -- Flag simples pra facilitar filtros de review negativa nos marts
        case
            when review_score <= 2 then true
            else false
        end as is_negative_review

    from source

)

select * from renamed