with ranked_properties as (
    select
        property_id,
        building_type,
        year_built,
        bedrooms,
        size,
        posted_on,
        row_number() over (
            partition by property_id
            order by posted_on desc, ingestion_ts desc
        ) as rn
    from {{ ref('stg_listings_enriched') }}
    where property_id is not null
)

select
    property_id,
    building_type,
    year_built,
    bedrooms,
    size
from ranked_properties
where rn = 1