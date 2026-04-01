{{
    config(
        materialized='incremental',
        unique_key='listing_id'
    )
}}

with base as (
    select
        listings.*,
        states.state
    from {{ ref('stg_listings') }} listings
    left join {{ ref('states') }} states
        on listings.city = states.city
),

deduped as (
    select *
    from base
    {% if is_incremental() %}
    where listing_id not in (
        select listing_id
        from {{ this }}
    )
    {% endif %}
)
select *
from deduped