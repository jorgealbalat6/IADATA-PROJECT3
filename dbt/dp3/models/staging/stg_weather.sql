with src as (
    select * from {{ source('airbnb_features', 'weather') }}
),
 
renamed as (
    select
        date,
        temp_max,
        temp_min,
        temp_mean,
        precipitation_mm,
        rain_mm,
        wind_max_kmh,
        weather_code,
        `source` as weather_source
    from src
    qualify row_number() over (
        partition by date
        order by case when `source` = 'historical' then 0 else 1 end
    ) = 1
)
 
select * from renamed