with dates as (
    select date
    from unnest(
        generate_date_array(current_date(), date_add(current_date(), interval 13 day))
    ) as date
),

events_agg as (
    select
        start_date as date,
        countif(category = 'sports')    as num_sports,
        countif(category = 'festivals') as num_festivals,
        sum(phq_attendance)             as total_attendance
    from {{ source('airbnb_features', 'events') }}
    group by start_date
),

holidays as (
    select distinct date
    from {{ source('airbnb_features', 'holidays') }}
    where applies_to_valencia = true
),

days_to_holiday as (
    select
        d.date,
        min(date_diff(h.date, d.date, day)) as days_to_next_holiday
    from dates d
    left join holidays h on h.date >= d.date
    group by d.date
)

select
    d.date,

    extract(month from d.date)                                    as month,
    extract(dayofweek from d.date)                                as day_of_week,
    if(extract(dayofweek from d.date) in (1, 7), 1, 0)           as is_weekend,
    if(h.date is not null, 1, 0)                                  as is_holiday,
    least(coalesce(dh.days_to_next_holiday, 14), 14)              as days_to_next_holiday,

    sin(2 * acos(-1) * extract(month from d.date) / 12)          as month_sin,
    cos(2 * acos(-1) * extract(month from d.date) / 12)          as month_cos,
    sin(2 * acos(-1) * extract(dayofweek from d.date) / 7)       as dow_sin,
    cos(2 * acos(-1) * extract(dayofweek from d.date) / 7)       as dow_cos,

    w.temp_mean,
    w.precipitation_mm,

    coalesce(e.num_sports, 0)                                     as num_sports,
    coalesce(e.num_festivals, 0)                                  as num_festivals,
    coalesce(e.total_attendance, 0)                                as total_attendance,
    if(coalesce(e.num_sports, 0) > 0, 1, 0)                      as has_sports_event,
    if(coalesce(e.num_festivals, 0) > 0, 1, 0)                   as has_festival,
    log(coalesce(e.total_attendance, 0) + 1)                      as log_attendance

from dates d
left join {{ source('airbnb_features', 'weather') }} w on d.date = w.date
left join events_agg e                                  on d.date = e.date
left join holidays h                                    on d.date = h.date
left join days_to_holiday dh                            on d.date = dh.date
order by d.date