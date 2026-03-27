WITH listings AS (SELECT * FROM {{ref('stg_listings_enriched')}})

SELECT distinct
    {{ dbt_utils.generate_surrogate_key(['listings.city']) }} AS location_key,
    listings.city,
    listings.state
FROM listings