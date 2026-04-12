-- =============================================================
-- 01_schema.sql — Schema fuente: Accidentes de tráfico (DATATRAN PRF)
-- Todos los campos se almacenan como TEXT para evitar fallos de
-- conversión de tipos. dbt se encargará del tipado final.
-- =============================================================

-- La base de datos es creada por 00_setup.sh usando MYSQL_DATABASE del .env.
-- El entrypoint de MySQL ejecuta este script con --database=${MYSQL_DATABASE},
-- por lo que no se necesita USE explícito.

-- Tabla raw: refleja exactamente las 30 columnas del CSV 2026.
-- Tipos TEXT preservan:
--   · km, latitude, longitude  → decimales con coma (ej: -7,291548)
--   · tracado_via              → valores múltiples separados con ; (ej: Aclive;Reta)
--   · valores 'NA'             → cadena literal (dbt aplica NULLIF)
--   · data_inversa / horario   → strings, dbt castea a DATE / TIME

DROP TABLE IF EXISTS accidentes_raw;

CREATE TABLE accidentes_raw (
    id                      TEXT,
    data_inversa            TEXT,
    dia_semana              TEXT,
    horario                 TEXT,
    uf                      TEXT,
    br                      TEXT,
    km                      TEXT,
    municipio               TEXT,
    causa_acidente          TEXT,
    tipo_acidente           TEXT,
    classificacao_acidente  TEXT,
    fase_dia                TEXT,
    sentido_via             TEXT,
    condicao_metereologica  TEXT,
    tipo_pista              TEXT,
    tracado_via             TEXT,   -- puede contener ; internos (ej: Aclive;Reta)
    uso_solo                TEXT,
    pessoas                 TEXT,
    mortos                  TEXT,
    feridos_leves           TEXT,
    feridos_graves          TEXT,
    ilesos                  TEXT,
    ignorados               TEXT,
    feridos                 TEXT,
    veiculos                TEXT,
    latitude                TEXT,   -- decimal con coma (ej: -7,291548)
    longitude               TEXT,   -- decimal con coma (ej: -48,286252)
    regional                TEXT,
    delegacia               TEXT,
    uop                     TEXT
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;
