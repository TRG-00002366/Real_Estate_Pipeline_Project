select listings.*, states.state
from {{ref('stg_listings')}} listings
LEFT JOIN {{ref('states')}} states
    on listings.city = states.CITY