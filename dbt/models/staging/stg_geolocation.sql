with source as (

    select * from {{ source('olist_raw', 'geolocation') }}

),

renamed as (

    select distinct
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state

    from source

)

select * from renamed