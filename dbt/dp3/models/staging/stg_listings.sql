with source as (
    select * from {{ source('airbnb_raw', 'listings') }}
),

renamed as (
    select
        id as listing_id,
        name,
        host_id,
        neighbourhood_cleansed,
        latitude,
        longitude,
        room_type,
        accommodates,
        bedrooms,
        beds,
        price as listing_price,
        minimum_nights,
        maximum_nights,
        number_of_reviews,
        review_scores_rating,
        review_scores_cleanliness,
        review_scores_location,
        review_scores_value,
        instant_bookable,
        snapshot_date
    from source
    qualify row_number() over (partition by id order by snapshot_date desc) = 1
)

select * from renamed