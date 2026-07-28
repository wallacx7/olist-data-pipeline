with geolocation as (
    select * from {{ ref('stg_geolocation') }}
),

aggregated as (
    select
        geolocation_zip_code_prefix as zip_code_prefix,
        avg(geolocation_lat) as latitude,
        avg(geolocation_lng) as longitude,
        -- um CEP pode ter múltiplas cidades/estados registrados;
        -- pega o mais frequente para representar o prefixo
        approx_top_count(geolocation_city, 1)[offset(0)].value as city,
        approx_top_count(geolocation_state, 1)[offset(0)].value as state

    from geolocation
    group by zip_code_prefix
)

select * from aggregated