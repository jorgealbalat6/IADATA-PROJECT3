-- Ocupación media por mes, barrio y tipo de habitación

with latest_snapshot as (
    select max(snapshot_date) as sd from {{ ref('stg_calendar') }}
),

cal as (
    select c.*
    from {{ ref('stg_calendar') }} c
    cross join latest_snapshot ls
    where c.snapshot_date = ls.sd
)

select
    l.neighbourhood_cleansed    as neighbourhood,
    l.room_type,
    extract(month from cal.date) as month,
    round(avg(cal.is_occupied), 4) as occupancy_rate,
    count(distinct cal.listing_id) as num_listings,
    round(avg(cal.price), 1)       as avg_price

from cal
join {{ ref('stg_listings') }} l on cal.listing_id = l.listing_id
group by 1, 2, 3