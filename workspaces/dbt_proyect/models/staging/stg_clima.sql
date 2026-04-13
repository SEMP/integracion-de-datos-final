with source as (
    select * from {{ source('datatran', 'clima_raw') }}
),

typed as (
    select
        -- Claves de join con accidentes
        round(cast(latitude  as double), 2)                         as lat_r,
        round(cast(longitude as double), 2)                         as lon_r,

        -- Timestamp y derivados
        cast("timestamp" as timestamp)                              as timestamp_utc,
        cast("timestamp" as date)                                   as fecha,
        hour(cast("timestamp" as timestamp))                        as hora,

        -- Código WMO y descripción
        try_cast(nullif(weather_code, '') as int)                   as weather_code,
        case try_cast(nullif(weather_code, '') as int)
            when 0  then 'Despejado'
            when 1  then 'Mayormente despejado'
            when 2  then 'Parcialmente nublado'
            when 3  then 'Nublado'
            when 45 then 'Niebla'
            when 48 then 'Niebla con escarcha'
            when 51 then 'Llovizna leve'
            when 53 then 'Llovizna moderada'
            when 55 then 'Llovizna intensa'
            when 61 then 'Lluvia leve'
            when 63 then 'Lluvia moderada'
            when 65 then 'Lluvia intensa'
            when 71 then 'Nevada leve'
            when 73 then 'Nevada moderada'
            when 75 then 'Nevada intensa'
            when 77 then 'Granizo'
            when 80 then 'Chubasco leve'
            when 81 then 'Chubasco moderado'
            when 82 then 'Chubasco intenso'
            when 85 then 'Chubasco de nieve leve'
            when 86 then 'Chubasco de nieve intenso'
            when 95 then 'Tormenta'
            when 96 then 'Tormenta con granizo leve'
            when 99 then 'Tormenta con granizo intenso'
            else        'Desconocido'
        end                                                         as weather_desc,

        -- Variables numéricas ERA5
        try_cast(nullif(precipitation,        '') as double)        as precipitation,
        try_cast(nullif(temperature_2m,       '') as double)        as temperature_2m,
        try_cast(nullif(relative_humidity_2m, '') as double)        as relative_humidity_2m,
        try_cast(nullif(dew_point_2m,         '') as double)        as dew_point_2m,
        try_cast(nullif(cloud_cover_low,      '') as int)           as cloud_cover_low,
        try_cast(nullif(wind_speed_10m,       '') as double)        as wind_speed_10m,
        try_cast(nullif(wind_gusts_10m,       '') as double)        as wind_gusts_10m,
        try_cast(nullif(shortwave_radiation,  '') as double)        as shortwave_radiation,
        try_cast(nullif(is_day,               '') as int) = 1       as is_day

    from source
)

select * from typed
