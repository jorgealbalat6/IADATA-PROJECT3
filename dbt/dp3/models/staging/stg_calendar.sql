with source as (
    select * from {{ source('airbnb_raw', 'calendar') }}
),

renamed as (
    select
        listing_id,
        date,
        snapshot_date,
        available,
        case when available = false then 1 else 0 end as is_occupied,
        price,
        adjusted_price,
        minimum_nights,
        maximum_nights
    from source
    where date is not null
      and listing_id is not null
)

select * from renamed