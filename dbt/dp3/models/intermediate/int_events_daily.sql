with events as (
    select * from {{ ref('stg_events') }}
),

aggregated as (
    select
        start_date as date,
        count(*) as num_events,
        sum(case when category = 'concerts' then 1 else 0 end) as num_concerts,
        sum(case when category = 'sports' then 1 else 0 end) as num_sports,
        sum(case when category = 'festivals' then 1 else 0 end) as num_festivals,
        max(phq_attendance) as max_attendance,
        sum(coalesce(phq_attendance, 0)) as total_attendance
    from events
    group by start_date
)

select * from aggregated