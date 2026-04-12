-- =============================================================
-- 03_clima_schema.sql — Schema fuente: Clima ERA5 (Open-Meteo)
--
-- Tabla raw generada por workspaces/scripts/extract_openmeteo.py.
-- Todos los campos se almacenan como TEXT para consistencia con
-- accidentes_raw. dbt aplica el tipado final.
--
-- Carga posterior (una vez generado el CSV):
--   LOAD DATA INFILE '/csv/clima_openmeteo.csv'
--   INTO TABLE clima_raw
--   CHARACTER SET utf8mb4
--   FIELDS TERMINATED BY ';' ENCLOSED BY '"'
--   LINES TERMINATED BY '\n'
--   IGNORE 1 LINES;
-- =============================================================

DROP TABLE IF EXISTS clima_raw;

CREATE TABLE clima_raw (
    latitude              TEXT,   -- coordenada redondeada a 2 decimales
    longitude             TEXT,   -- coordenada redondeada a 2 decimales
    timestamp             TEXT,   -- formato ISO 8601: 2026-01-01T00:00

    -- Variables ERA5 (Open-Meteo)
    precipitation         TEXT,   -- mm/h
    weather_code          TEXT,   -- código WMO
    temperature_2m        TEXT,   -- °C
    relative_humidity_2m  TEXT,   -- %
    dew_point_2m          TEXT,   -- °C — riesgo de escarcha/niebla
    cloud_cover_low       TEXT,   -- % nubes bajas (proxy de niebla)
    wind_speed_10m        TEXT,   -- km/h
    wind_gusts_10m        TEXT,   -- km/h
    shortwave_radiation   TEXT,   -- W/m² (encandilamiento solar)
    is_day                TEXT    -- 1 = día, 0 = noche
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;
