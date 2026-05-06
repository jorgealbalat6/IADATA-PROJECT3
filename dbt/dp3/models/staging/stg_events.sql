with source as (
    select * from {{ source('airbnb_features', 'events') }}
),

renamed as (
    select
        event_id,
        title,
        category,
        start_date,
        end_date,
        duration_days,
        latitude,
        longitude,
        rank,
        local_rank,
        phq_attendance
    from source
    where start_date is not null
)

select * from renamed