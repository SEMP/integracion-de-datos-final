{{
    config(
        materialized = 'table',
        indexes = [
            {'columns': ['id'],            'unique': true},
            {'columns': ['data_inversa']},
            {'columns': ['uf']},
            {'columns': ['causa_acidente']},
        ]
    )
}}

select
    -- Identificación
    id,
    data_inversa,
    dia_semana,
    horario,
    fase_dia,

    -- Localización
    uf,
    municipio,
    br,
    km,
    latitude,
    longitude,

    -- Causa y tipo
    causa_acidente,
    tipo_acidente,
    classificacao_acidente,

    -- Características de la vía
    sentido_via,
    tipo_pista,
    tracado_via,
    uso_solo,

    -- Víctimas y vehículos
    pessoas,
    mortos,
    feridos_leves,
    feridos_graves,
    ilesos,
    ignorados,
    feridos,
    veiculos,

    -- Clima según PRF
    condicao_metereologica,

    -- Clima ERA5 (Open-Meteo)
    clima_precipitation,
    clima_weather_code,
    clima_weather_desc,
    clima_temperature_2m,
    clima_relative_humidity_2m,
    clima_dew_point_2m,
    clima_cloud_cover_low,
    clima_wind_speed_10m,
    clima_wind_gusts_10m,
    clima_shortwave_radiation,
    clima_is_day,
    clima_join_match

from {{ ref('int_accidentes_clima') }}
