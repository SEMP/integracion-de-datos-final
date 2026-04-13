with source as (
    select * from {{ source('datatran', 'accidentes_raw') }}
),

renamed as (
    select
        -- Surrogate key propio (DATATRAN id no es confiable como único)
        row_number() over (order by id, data_inversa, horario) as id_accidente,

        -- ID original de DATATRAN (informativo, sin restricción de unicidad)
        id                                                                          as id_datatran,

        -- Identificación
        cast(data_inversa as date)                                                  as data_inversa,
        dia_semana,
        cast(horario as time)                                                       as horario,
        fase_dia,

        -- Localización
        uf,
        municipio,
        br,
        try_cast(replace(nullif(km,        'NA'), ',', '.') as decimal(10, 3))     as km,
        try_cast(replace(nullif(latitude,  'NA'), ',', '.') as double)             as latitude,
        try_cast(replace(nullif(longitude, 'NA'), ',', '.') as double)             as longitude,
        round(try_cast(replace(nullif(latitude,  'NA'), ',', '.') as double), 2)   as lat_r,
        round(try_cast(replace(nullif(longitude, 'NA'), ',', '.') as double), 2)   as lon_r,

        -- Causa y tipo
        causa_acidente,
        tipo_acidente,
        nullif(classificacao_acidente, 'NA')                                        as classificacao_acidente,

        -- Características de la vía
        sentido_via,
        tipo_pista,
        tracado_via,
        uso_solo,

        -- Víctimas y vehículos
        try_cast(nullif(pessoas,        'NA') as int)                               as pessoas,
        try_cast(nullif(mortos,         'NA') as int)                               as mortos,
        try_cast(nullif(feridos_leves,  'NA') as int)                               as feridos_leves,
        try_cast(nullif(feridos_graves, 'NA') as int)                               as feridos_graves,
        try_cast(nullif(ilesos,         'NA') as int)                               as ilesos,
        try_cast(nullif(ignorados,      'NA') as int)                               as ignorados,
        try_cast(nullif(feridos,        'NA') as int)                               as feridos,
        try_cast(nullif(veiculos,       'NA') as int)                               as veiculos,

        -- Clima según PRF
        condicao_metereologica,

        -- Administrativo
        regional,
        delegacia,
        uop

    from source
)

select * from renamed
