# Progreso - Trabajo Final: Pipeline ELT con Datos de Accidentes de Tráfico

## Objetivo

Implementar un pipeline ELT completo con al menos 2 fuentes de datos, modelo dimensional
justificado, tests de calidad con dbt-expectations, dashboard en Metabase y reporte técnico.

Arquitectura: MySQL -> Airbyte -> MotherDuck -> dbt -> Metabase, orquestado con Prefect.

---

## Fuentes de datos

### Fuente 1: Accidentes de tráfico en Brasil (DATATRAN - PRF)
- **Origen:** Policía Rodoviária Federal de Brasil (dados.gov.br)
- **Formato:** CSV con separador `;` y encoding Latin1/ISO-8859-1
- **Archivo:** `datatran2026.csv` — ~11,380 registros, año 2026
- **Columnas clave:**
  - `id`, `data_inversa` (fecha), `horario`, `uf` (estado), `municipio`
  - `causa_acidente`, `tipo_acidente`, `classificacao_acidente`
  - `fase_dia`, `condicao_metereologica`
  - `mortos`, `feridos_leves`, `feridos_graves`, `ilesos`, `veiculos`
  - `latitude`, `longitude`
- **Nota de carga:** separador `;`, encoding Latin1, `tracado_via` contiene `;` internos (entrecomillado)

### Fuente 2: Clima histórico (Open-Meteo ERA5) — DECIDIDO
- **Origen:** Open-Meteo Historical Weather API (ERA5 reanalysis)
- **URL base:** `https://archive-api.open-meteo.com/v1/archive`
- **Costo:** Gratuito, sin API key
- **Join con fuente 1:** `data_inversa` + `horario` + `latitude` + `longitude`
- **Variables principales:** `precipitation`, `weather_code` (WMO), `temperature_2m`,
  `relative_humidity_2m`, `cloud_cover_low`, `wind_gusts_10m`, `is_day`, `shortwave_radiation`

**Decisión Open-Meteo vs OpenWeather:** Se eligió Open-Meteo ERA5 por ser gratuito, sin API key,
y proveer más variables útiles (ráfagas, nubes bajas, radiación solar). Ver análisis completo en
`Verificaciondesuscripcion_Extracciondedatos.ipynb`.

---

## Estructura del workspace

```
Trabajo_Final/
├── data/                        # CSV convertidos a UTF-8 (gitignored)
├── docs/
│   ├── PROGRESO.md
│   ├── Grilla_Evaluacion_TP_Final.xlsx
│   ├── Verificaciondesuscripcion_Extracciondedatos.ipynb
│   └── typst/
│       ├── main.typ             # Reporte técnico
│       ├── refs.bib
│       └── assets/
└── workspaces/
    ├── scripts/
    │   ├── convert_csv_to_utf8.py
    │   ├── extract_openmeteo.py # Extracción ERA5 con reanudación
    │   └── example.env
    ├── containers/
    │   ├── Dockerfile
    │   ├── docker-compose.yaml
    │   ├── example.env
    │   └── initdb/
    │       ├── 00_setup.sh      # Crea BDs y permisos (se ejecuta primero)
    │       ├── 01_schema.sql    # CREATE TABLE accidentes_raw (todo TEXT)
    │       ├── 02_load_data.sql # Referencia histórica (carga gestionada por Prefect)
    │       └── 03_clima_schema.sql # CREATE TABLE clima_raw
    ├── dbt_proyect/
    │   ├── dbt_project.yml
    │   ├── profiles.yml
    │   ├── packages.yml
    │   ├── macros/generate_schema_name.sql
    │   └── models/
    │       ├── sources.yml
    │       ├── staging/
    │       │   ├── stg_accidentes.sql
    │       │   ├── stg_clima.sql
    │       │   └── schema.yml
    │       ├── intermediate/
    │       │   └── int_accidentes_clima.sql
    │       └── marts/
    │           ├── obt_accidentes.sql
    │           └── schema.yml
    └── prefect/
        ├── pipeline.py
        └── example.env
```

---

## Modelo dimensional

Se eligió el patrón **One Big Table (OBT)** en lugar de esquema estrella Kimball. Con ~11.380 filas el volumen no justifica la normalización, y el análisis exploratorio en Metabase se beneficia de una sola tabla ancha. La capa intermedia ya resuelve el join con ERA5.

Capas implementadas:
- `staging/stg_accidentes` — tipado, NULLIF, ROUND lat/lon, surrogate key `id_accidente`
- `staging/stg_clima` — tipado, decodificación WMO, join keys `lat_r/lon_r/fecha/hora`
- `intermediate/int_accidentes_clima` — LEFT JOIN por `(lat_r, lon_r, data_inversa, hora)`
- `marts/obt_accidentes` — tabla ancha final consumida por Metabase

---

## Estado general

| Componente | Estado | Detalle |
|---|---|---|
| Definición de fuentes de datos | ✅ Listo | DATATRAN 2026 + Open-Meteo ERA5 |
| Pre-procesamiento CSV | ✅ Listo | `convert_csv_to_utf8.py` con `.env`, salida a `data/` |
| Infraestructura Docker | ✅ Listo | `tf-mysql`, `tf-phpmyadmin`, `tf-metabase` operativos |
| Metabase — conexión MotherDuck | ✅ Listo | `md:airbyte_trabajo`, driver DuckDB configurado |
| Airbyte — source MySQL + destination MotherDuck | ✅ Listo | `MySQL_Datatran → MotherDuck_datatran` sincronizando |
| Airbyte — clima_raw sync | ✅ Listo | `clima_raw` agregado como segundo stream, sync exitoso |
| Prefect pipeline | ✅ Listo | 7 tasks, ejecución end-to-end exitosa |
| Definición del modelo dimensional | ✅ Listo | OBT: staging → intermediate (join ERA5) → marts/obt_accidentes |
| dbt modelos staging | ✅ Listo | `stg_accidentes`, `stg_clima` con tipado y surrogate key |
| dbt modelos intermediate | ✅ Listo | `int_accidentes_clima`: LEFT JOIN por lat_r/lon_r/fecha/hora |
| dbt modelos marts | ✅ Listo | `obt_accidentes`: tabla ancha final materializada como table |
| dbt tests (dbt-expectations) | ✅ Listo | 15/15 tests pasan, 0 warnings |
| Reporte técnico (Typst) | 🔄 En progreso | Pasos 1–5 documentados, pendiente resultados finales |
| Metabase dashboard | ✅ Listo | 5 visualizaciones implementadas sobre `obt_accidentes` |
| Video presentación | ⏳ Pendiente | — |

---

## Checklist de entregables (Grilla de Evaluación)

### Infraestructura base

- [x] `docker-compose.yaml` con MySQL 8.0 + phpMyAdmin + Metabase (`tf-*`)
- [x] `initdb/00_setup.sh` — crea BDs y otorga permisos desde variables de entorno
- [x] `initdb/01_schema.sql` — schema `accidentes_raw` (todo TEXT)
- [x] `initdb/02_load_data.sql` — `LOAD DATA INFILE` con `ENCLOSED BY '"'`
- [x] `example.env` con todas las variables requeridas
- [x] `docker compose up -d` ejecutado y datos verificados (11.380 registros)
- [x] `workspaces/scripts/convert_csv_to_utf8.py` con `.env` y salida a `data/`

### 1. Extracción con Airbyte — 15 pts

| Criterio | Pts | Estado |
|---|---|---|
| Al menos 2 fuentes de datos configuradas correctamente | 5 | 🔄 En progreso |
| Conexión al Data Warehouse (MotherDuck) funcionando | 4 | ✅ Listo |
| Todas las tablas necesarias sincronizadas | 4 | 🔄 En progreso |
| Sync mode apropiado para el caso de uso | 2 | ✅ Listo |

- [x] Source MySQL (`MySQL_Datatran`, base `tf-datatran`) configurado en Airbyte
- [x] Destination MotherDuck (`MotherDuck_datatran`, `md:airbyte_trabajo`, schema `datatran`) configurado
- [x] Connection `MySQL_Datatran → MotherDuck_datatran` creada y sync exitoso (11.380 registros, 2m 5s)
- [x] Sync mode: Full Refresh | Overwrite justificado (dataset estático, sin columnas de timestamp)
- [x] Script `extract_openmeteo.py` ejecutado → `data/clima_openmeteo.csv` generado (53/59 fechas completas; 6 con errores de API marcadas en `.failed`)
- [x] Tabla `clima_raw` cargada en MySQL vía pipeline Prefect (`load_clima_raw`)
- [x] Segundo stream `clima_raw` (13 campos) agregado a la connection Airbyte
- [x] Sync final con ambas tablas (`accidentes_raw` + `clima_raw`) completado exitosamente

### 2. Modelado y Transformación con dbt — 25 pts

| Criterio | Pts | Estado |
|---|---|---|
| Proyecto dbt estructurado correctamente (staging/, marts/) | 3 | ✅ Listo |
| Sources definidos en YAML con database y schema | 2 | ✅ Listo |
| Modelos staging con limpieza y tipado de datos | 5 | ✅ Listo |
| Modelo dimensional (Kimball) o OBT implementado | 6 | ✅ Listo |
| Justificación del enfoque de modelado elegido | 4 | ✅ Listo |
| Uso correcto de `ref()` y `source()` para dependencias | 2 | ✅ Listo |
| Materializations apropiados (view/table/incremental) | 3 | ✅ Listo |

- [x] Proyecto `dbt_proyect` inicializado, `profiles.yml` apuntando a `md:airbyte_trabajo`
- [x] `sources.yml` con `database: airbyte_trabajo` y `schema: datatran`
- [x] `stg_accidentes`: `NULLIF`, `REPLACE` comas decimales, `CAST` de tipos, surrogate key `id_accidente`, columnas `lat_r`/`lon_r`
- [x] `stg_clima`: tipado directo, deduplicación por row_number, decodificación WMO, columnas join `lat_r`/`lon_r`/`fecha`/`hora`
- [x] `int_accidentes_clima`: LEFT JOIN por `(lat_r, lon_r, data_inversa, hora)`
- [x] `obt_accidentes`: tabla ancha final (OBT) en marts, `clima_join_match` indica si hubo dato ERA5
- [x] Materializations: `view` en staging/intermediate, `table` en marts
- [x] `dbt run` exitoso sin errores

### 3. Calidad de Datos con Testing — 15 pts

| Criterio | Pts | Estado |
|---|---|---|
| Tests `unique` y `not_null` en primary keys | 3 | ✅ Listo |
| Mínimo 5 tests de `dbt-expectations` implementados | 6 | ✅ Listo |
| Tests cubren diferentes dimensiones de calidad | 3 | ✅ Listo |
| Todos los tests pasan exitosamente | 3 | ✅ Listo |

- [x] `unique` + `not_null` en `stg_accidentes.id_accidente` y `obt_accidentes.id_accidente`
- [x] `expect_column_values_to_be_between` en `mortos` (0–100) y `latitude` (-34 a 6)
- [x] `not_null` en `stg_accidentes.uf` y `stg_accidentes.data_inversa`
- [x] `expect_column_proportion_of_unique_values_to_be_between` en `municipio`
- [x] `expect_column_values_to_be_between` en `data_inversa` (rango 2026-01-01 a 2026-12-31)
- [x] `dbt test`: **15/15 tests pasan, 0 warnings**

### 4. Orquestación con Prefect — 12 pts

| Criterio | Pts | Estado |
|---|---|---|
| Pipeline definido con decoradores `@flow` y `@task` | 3 | ✅ Listo |
| Integración con Airbyte (API o SDK) | 4 | ✅ Listo |
| Integración con dbt (`prefect-dbt`) | 3 | ✅ Listo |
| Manejo de errores y logging apropiado | 2 | ✅ Listo |

- [x] `workspaces/prefect/pipeline.py` con 7 tasks usando `@flow` y `@task`
- [x] Task `ensure_containers_up`: levanta Docker y espera MySQL ready (reintentos: 2)
- [x] Task `load_accidentes_raw`: convierte CSVs (convert_csv_to_utf8.py), recrea tabla vía 01_schema.sql y carga todos los `*_utf8.csv` con LOAD DATA LOCAL INFILE
- [x] Task `extract_openmeteo`: reanudación vía `.progress`/`.failed`, continúa aunque haya fechas fallidas
- [x] Task `load_clima_raw`: crea tabla y carga CSV; saltea si ya tiene datos
- [x] Task `airbyte_sync`: dispara sync vía API REST con Basic Auth, polling cada 10 s (timeout: 10 min)
- [x] Tasks `dbt_run` y `dbt_test`: ejecutan dbt con `--profiles-dir .` como subproceso con logging
- [x] Ejecución end-to-end exitosa: 7/7 tasks completadas, 15/15 dbt tests pasan

### 5. Visualización con Metabase — 15 pts

| Criterio | Pts | Estado |
|---|---|---|
| Conexión al Data Warehouse funcionando | 2 | ✅ Listo |
| Mínimo 5 visualizaciones implementadas | 5 | ✅ Listo |
| Visualizaciones responden preguntas de negocio claras | 4 | ✅ Listo |
| Dashboard organizado con filtros interactivos | 4 | ✅ Listo |

- [x] Conexión a `md:airbyte_trabajo` configurada (`Motherduck_Trabajo_Final`)
- [x] Dashboard unificado con 5 paneles sobre `marts.obt_accidentes`
- [x] Panel 1 (dona): top 5 causas de accidentes con participación porcentual
- [x] Panel 2 (barras): distribución de accidentes por día de la semana
- [x] Panel 3 (líneas doble eje): accidentes y muertes por día de la semana
- [x] Panel 4 (dona): accidentes por condición climática PRF
- [x] Panel 5 (barras agrupadas): muertes, heridos leves y graves por condición climática
- [x] Filtros interactivos: Estado (UF) y Fechas, propagados a todos los paneles
- [x] Capturas sin filtros (11.380 accidentes) y filtrada por RJ incluidas en `docs/typst/assets/`

### 6. Reporte Técnico — 10 pts

| Criterio | Pts | Estado |
|---|---|---|
| Estructura correcta (intro, metodología, resultados, conclusión) | 3 | 🔄 En progreso |
| Documentación clara del pipeline y decisiones de diseño | 4 | 🔄 En progreso |
| Instrucciones de ejecución reproducibles | 3 | ✅ Listo |

- [x] Secciones introducción y metodología redactadas (`docs/typst/main.typ`)
- [x] Decisiones de diseño documentadas (fuentes, encoding, TEXT en MySQL, Open-Meteo vs OpenWeather)
- [x] Instrucciones reproducibles en `README.md` y `main.typ`
- [ ] Completar sección de resultados (capturas Airbyte, dbt, Prefect, Metabase)
- [ ] Exportar a PDF

### 7. Video Explicativo — 8 pts

| Criterio | Pts | Estado |
|---|---|---|
| Demostración del pipeline funcionando end-to-end | 4 | ⏳ Pendiente |
| Explicación clara de la arquitectura y decisiones técnicas | 4 | ⏳ Pendiente |

- [ ] Grabación del pipeline completo en ejecución
- [ ] Narración explicando arquitectura y decisiones técnicas

---

## Puntaje total: 100 pts

| Componente | Pts máx | Obtenido |
|---|---|---|
| 1. Extracción (Airbyte) | 15 | ~15 (2 sources + destination + ambas tablas sincronizadas ✅) |
| 2. Modelado y Transformación (dbt) | 25 | ~25 (staging + intermediate + OBT + fuente/ref/materializations ✅) |
| 3. Calidad de Datos (Testing) | 15 | ~15 (15/15 tests pasan, 5+ dbt-expectations ✅) |
| 4. Orquestación (Prefect) | 12 | ~12 (7 tasks, ejecución end-to-end exitosa ✅) |
| 5. Visualización (Metabase) | 15 | ~15 (5 visualizaciones implementadas ✅) |
| 6. Reporte Técnico | 10 | ~6 (pasos 1–5 documentados, faltan resultados finales) |
| 7. Video Explicativo | 8 | — |
| **TOTAL** | **100** | **~90** |

---

## Pasos siguientes (orden sugerido)

### 1. Reporte técnico — completar resultados

- Agregar capturas de: Prefect UI (flow run exitoso), `dbt test` (15/15)
- Exportar a PDF: `typst compile docs/typst/main.typ`

### 2. Video

Grabar demostración end-to-end: ejecución del pipeline Prefect, Airbyte sync, dbt tests, y navegación del dashboard.

