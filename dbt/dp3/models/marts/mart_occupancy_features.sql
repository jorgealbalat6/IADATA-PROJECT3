with calendar as (
    select * from {{ ref('int_calendar') }}
),

listings as (
    select * from {{ ref('stg_listings') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

events as (
    select * from {{ ref('int_events_daily') }}
),

holidays as (
    select * from {{ ref('stg_holidays') }}
),

joined as (
    select
        -- Target
        c.is_occupied,

        -- Temporales
        c.date,
        c.day_of_week,
        c.month,
        c.year,
        c.week_of_year,
        c.is_weekend,
        c.days_to_next_holiday,

        -- Del piso
        c.listing_id,
        l.neighbourhood_cleansed,
        l.room_type,
        l.accommodates,
        l.bedrooms,
        l.beds,
        l.listing_price,
        l.minimum_nights,
        l.number_of_reviews,
        l.review_scores_rating,
        l.review_scores_cleanliness,
        l.review_scores_location,
        l.review_scores_value,
        l.instant_bookable,
        l.latitude,
        l.longitude,

        -- Meteorologia
        w.temp_max,
        w.temp_min,
        w.temp_mean,
        w.precipitation_mm,
        w.rain_mm,
        w.wind_max_kmh,
        w.weather_code,

        -- Eventos
        coalesce(e.num_events, 0) as num_events,
        coalesce(e.num_concerts, 0) as num_concerts,
        coalesce(e.num_sports, 0) as num_sports,
        coalesce(e.num_festivals, 0) as num_festivals,
        coalesce(e.max_attendance, 0) as max_attendance,
        coalesce(e.total_attendance, 0) as total_attendance,

        -- Festivos
        if(h.date is not null, 1, 0) as is_holiday,
        h.local_name as holiday_name

    from calendar c
    left join listings l on c.listing_id = l.listing_id
    left join weather w on c.date = w.date
    left join events e on c.date = e.date
    left join holidays h on c.date = h.date
    where c.date between '2025-06-12' and '2026-04-30'
)

select * from joined