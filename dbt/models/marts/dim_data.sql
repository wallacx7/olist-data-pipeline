with date_spine as (

    select
        date_day
    from unnest(
        generate_date_array(
            (select min(date(order_purchase_timestamp)) from {{ ref('stg_orders') }}),
            (select max(date(order_purchase_timestamp)) from {{ ref('stg_orders') }}),
            interval 1 day
        )
    ) as date_day

),

final as (

    select
        date_day,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(quarter from date_day) as quarter,
        extract(dayofweek from date_day) as day_of_week,
        format_date('%B', date_day) as month_name,
        format_date('%A', date_day) as day_name,
        case
            when extract(dayofweek from date_day) in (1, 7) then true
            else false
        end as is_weekend

    from date_spine

)

select * from final