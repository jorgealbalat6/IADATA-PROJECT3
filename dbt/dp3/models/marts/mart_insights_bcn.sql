-- Estadísticas globales de Barcelona por tipo de habitación

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
        l.room_type,
        l.listing_price,
        l.accommodates,
        avg(cal.is_occupied) as listing_occupancy
    from cal
    join {{ ref('stg_listings') }} l on cal.listing_id = l.listing_id
    where l.listing_price > 0
    group by 1, 2, 3, 4
)

select
    room_type,
    count(distinct listing_id)        as num_listings,
    round(avg(listing_occupancy), 4)  as avg_occupancy,
    round(avg(listing_price), 1)      as avg_price,
    round(approx_quantiles(listing_price, 100)[offset(50)], 1) as median_price,
    round(avg(accommodates), 1)       as avg_accommodates
from listing_occ
group by 1