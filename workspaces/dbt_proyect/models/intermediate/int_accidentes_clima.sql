with accidentes as (
    select * from {{ ref('stg_accidentes') }}
),

clima as (
    select * from {{ ref('stg_clima') }}
),

joined as (
    select
        -- Campos de accidentes
        a.id_accidente,
        a.id_datatran,
        a.data_inversa,
        a.dia_semana,
        a.horario,
        a.fase_dia,
        a.uf,
        a.municipio,
        a.br,
        a.km,
        a.latitude,
        a.longitude,
        a.causa_acidente,
        a.tipo_acidente,
        a.classificacao_acidente,
        a.sentido_via,
        a.tipo_pista,
        a.tracado_via,
        a.uso_solo,
        a.pessoas,
        a.mortos,
        a.feridos_leves,
        a.feridos_graves,
        a.ilesos,
        a.ignorados,
        a.feridos,
        a.veiculos,
        a.condicao_metereologica,
        a.regional,
        a.delegacia,
        a.uop,

        -- Campos ERA5 con prefijo clima_
        c.precipitation         as clima_precipitation,
        c.weather_code          as clima_weather_code,
        c.weather_desc          as clima_weather_desc,
        c.temperature_2m        as clima_temperature_2m,
        c.relative_humidity_2m  as clima_relative_humidity_2m,
        c.dew_point_2m          as clima_dew_point_2m,
        c.cloud_cover_low       as clima_cloud_cover_low,
        c.wind_speed_10m        as clima_wind_speed_10m,
        c.wind_gusts_10m        as clima_wind_gusts_10m,
        c.shortwave_radiation   as clima_shortwave_radiation,
        c.is_day                as clima_is_day,

        -- Flag de match
        c.lat_r is not null     as clima_join_match

    from accidentes a
    left join clima c
        on  a.lat_r        = c.lat_r
        and a.lon_r        = c.lon_r
        and a.data_inversa = c.fecha
        and hour(a.horario) = c.hora
)

select * from joined
