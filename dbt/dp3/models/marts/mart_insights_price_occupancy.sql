-- Relación precio-ocupación por barrio y tipo de habitación
-- Agrupa listings en rangos de precio para ver si los más baratos se alquilan más

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
        avg(cal.is_occupied) as listing_occupancy
    from cal
    join {{ ref('stg_listings') }} l on cal.listing_id = l.listing_id
    where l.listing_price > 0
    group by 1, 2, 3, 4
),

with_bucket as (
    select
        *,
        case
            when listing_price < 30  then '0-30'
            when listing_price < 60  then '30-60'
            when listing_price < 90  then '60-90'
            when listing_price < 120 then '90-120'
            when listing_price < 160 then '120-160'
            when listing_price < 200 then '160-200'
            when listing_price < 300 then '200-300'
            else '300+'
        end as price_bucket,
        case
            when listing_price < 30  then 1
            when listing_price < 60  then 2
            when listing_price < 90  then 3
            when listing_price < 120 then 4
            when listing_price < 160 then 5
            when listing_price < 200 then 6
            when listing_price < 300 then 7
            else 8
        end as bucket_order
    from listing_occ
)

select
    neighbourhood,
    room_type,
    price_bucket,
    bucket_order,
    count(*) as num_listings,
    round(avg(listing_occupancy), 4)  as avg_occupancy,
    round(avg(listing_price), 1)      as avg_price
from with_bucket
group by 1, 2, 3, 4
order by 1, 2, 4
