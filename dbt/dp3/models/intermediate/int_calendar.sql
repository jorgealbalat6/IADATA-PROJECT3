with calendar as (
    select * from {{ ref('stg_calendar') }}
),

enriched as (
    select
        listing_id,
        date,
        snapshot_date,
        is_occupied,
        price,
        minimum_nights,

        extract(dayofweek from date) as day_of_week,
        extract(month from date) as month,
        extract(year from date) as year,
        extract(week from date) as week_of_year,
        if(extract(dayofweek from date) in (1, 7), 1, 0) as is_weekend,

        date_diff(
            (select min(h.date) from {{ ref('stg_holidays') }} h where h.date >= calendar.date),
            date,
            day
        ) as days_to_next_holiday

    from calendar
)

select * from enriched