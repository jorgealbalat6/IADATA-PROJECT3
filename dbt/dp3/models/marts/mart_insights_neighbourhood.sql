-- Stats agregados por barrio y tipo de habitación (con percentiles de precio)

with latest_snapshot as (
    select max(snapshot_date) as sd from {{ ref('stg_calendar') }}
),

cal as (
    select c.*
    from {{ ref('stg_calendar') }} c
    cross join latest_snapshot ls
    where c.snapshot_date = ls.sd
),

listing_occ as (
    select
        cal.listing_id,
        l.neighbourhood_cleansed  as neighbourhood,
        l.room_type,
        l.listing_price,
        l.accommodates,
        l.bedrooms,
        l.beds,
        l.minimum_nights,
        l.instant_bookable,
        l.number_of_reviews,
        avg(cal.is_occupied) as listing_occupancy
    from cal
    join {{ ref('stg_listings') }} l on cal.listing_id = l.listing_id
    where l.listing_price > 0
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)

select
    neighbourhood,
    room_type,
    count(distinct listing_id)                as num_listings,
    round(avg(listing_occupancy), 4)          as avg_occupancy,
    round(avg(listing_price), 1)              as avg_price,
    round(min(listing_price), 1)              as min_price,
    round(max(listing_price), 1)              as max_price,
    round(approx_quantiles(listing_price, 100)[offset(10)], 1)  as p10_price,
    round(approx_quantiles(listing_price, 100)[offset(25)], 1)  as p25_price,
    round(approx_quantiles(listing_price, 100)[offset(50)], 1)  as median_price,
    round(approx_quantiles(listing_price, 100)[offset(75)], 1)  as p75_price,
    round(approx_quantiles(listing_price, 100)[offset(90)], 1)  as p90_price,
    round(avg(accommodates), 1)               as avg_accommodates,
    round(avg(bedrooms), 1)                   as avg_bedrooms,
    round(avg(beds), 1)                       as avg_beds,
    round(avg(minimum_nights), 1)             as avg_minimum_nights,
    round(avg(number_of_reviews), 0)          as avg_reviews,
    round(countif(instant_bookable) / count(*), 4) as pct_instant_bookable
from listing_occ
group by 1, 2