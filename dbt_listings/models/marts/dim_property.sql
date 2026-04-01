WITH properties AS (SELECT property_id,building_type,year_built,bedrooms,size FROM {{ref('stg_listings_enriched')}})

SELECT distinct
    {{ dbt_utils.generate_surrogate_key(['properties.property_id']) }} AS property_key,
    property_id,
    building_type,
    year_built,
    bedrooms,
    properties.size
FROM properties
