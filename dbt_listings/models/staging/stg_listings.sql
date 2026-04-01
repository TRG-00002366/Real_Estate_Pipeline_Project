{{
    config(
        materialized='view'
    )
}}

with source as (

    select *
    from {{ source('bronze', 'raw_listings') }}

),

parsed as (

    select
        parse_json(raw_data:"$1".message::string) as msg,
        ingestion_ts,
        source_file
    from source

),

silver_data as (

    select
        {{ dbt_utils.generate_surrogate_key([
            "(msg:property_id)::string",
            "(msg:customer_id)::string",
            "(msg:posted_on)::string"
        ]) }} as listing_id,
        (msg:property_id)::number      as property_id,
        (msg:customer_id)::number      as customer_id,
        (msg:building_type)::string    as building_type,
        (msg:year_built)::number       as year_built,
        (msg:posted_on)::timestamp_ntz as posted_on,
        (msg:rented_on)::timestamp_ntz as rented_on,
        (msg:bedrooms)::number         as bedrooms,
        (msg:rent)::number             as rent,
        (msg:size)::number             as size,
        (msg:duration)::number         as duration,
        (msg:city)::string             as city,
        (msg:rental_status)::string    as rental_status,
        ingestion_ts,
        source_file
    from parsed
    where (msg:property_id)::number is not null

)

select *
from silver_data