with source as (
    select * from {{ source('airbnb_features', 'holidays') }}
),
 
renamed as (
    select
        date,
        local_name,
        name,
        is_national,
        applies_to_valencia as applies_to_barcelona,
        holiday_type
    from source
    where applies_to_valencia = true
)
 
select * from renamed