WITH properties AS (SELECT * FROM {{ref('stg_properties_deduped')}})

SELECT distinct
    {{ dbt_utils.generate_surrogate_key(['properties.property_id']) }} AS property_key,
    property_id,
    building_type,
    year_built,
    bedrooms,
    properties.size
FROM properties
