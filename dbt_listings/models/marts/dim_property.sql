WITH listings AS (SELECT * FROM {{ref('stg_listings_enriched')}})

SELECT 
    {{ dbt_utils.generate_surrogate_key(['listings.property_id']) }} AS property_key,
    property_id,
    building_type,
    year_built,
    bedrooms,
    listings.size
FROM listings
