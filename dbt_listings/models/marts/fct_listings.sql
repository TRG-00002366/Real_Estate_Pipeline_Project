{{ config(
    materialized='incremental',
    unique_key='listing_id'
) }}

WITH listings AS (
    SELECT *
    FROM {{ ref('stg_listings_enriched') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (
            SELECT MAX(ingestion_ts)
            FROM {{ this }}
        )
    {% endif %}
),

locations AS (
    SELECT city, state, location_key FROM {{ ref('dim_location') }}
),

properties AS (
    SELECT property_id, property_key FROM {{ ref('dim_property') }}
),

dates AS (
    SELECT FULL_DATE, date_key FROM {{ ref('dim_date') }}
)

SELECT
    l.listing_id,
    pd.date_key AS posted_date_key,
    rd.date_key as rented_date_key,
    loc.location_key,
    p.property_key,
    l.rent,
    l.duration,
    l.rental_status,
    l.ingestion_ts
FROM listings l
LEFT JOIN locations loc ON l.city = loc.city AND l.state = loc.state
LEFT JOIN properties p ON p.property_id = l.property_id
LEFT JOIN dates pd ON CAST(l.posted_on AS DATE) = pd.FULL_DATE
LEFT JOIN dates rd ON CAST(l.rented_on AS DATE) = rd.FULL_DATE