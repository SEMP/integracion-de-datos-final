-- =============================================================
-- 02_load_data.sql — Carga del CSV DATATRAN 2026 a MySQL
--
-- PRE-REQUISITO: ejecutar antes de iniciar el contenedor:
--   python3 workspaces/scripts/convert_csv_to_utf8.py
-- El script genera datatran2026_utf8.csv en el mismo directorio
-- que el CSV original. Asegurarse de que CSV_DIR en .env apunte
-- a ese directorio.
--
-- El CSV original tiene:
--   · Encoding: Latin-1  →  convertido a UTF-8 por el script
--   · Separador de campos: ;
--   · Separador decimal:   , (lat/lon/km)
--   · Encabezado: fila 1  →  IGNORE 1 LINES
--   · tracado_via contiene ; internos, siempre entre comillas
--   · Valores nulos: cadena 'NA'  →  dbt convierte con NULLIF
--   · Fin de línea: LF (\n) tras la conversión
-- =============================================================

USE datatran;

LOAD DATA INFILE '/csv/datatran2026_utf8.csv'
INTO TABLE accidentes_raw
CHARACTER SET utf8mb4
FIELDS
    TERMINATED BY ';'
    ENCLOSED BY '"'
LINES
    TERMINATED BY '\n'
IGNORE 1 LINES;
