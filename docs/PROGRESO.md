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
    │   └── example.env
    ├── containers/
    │   ├── Dockerfile
    │   ├── docker-compose.yaml
    │   ├── example.env
    │   └── initdb/
    │       ├── 00_setup.sh      # Crea BDs y permisos (se ejecuta primero)
    │       ├── 01_schema.sql    # CREATE TABLE accidentes_raw (todo TEXT)
    │       └── 02_load_data.sql # LOAD DATA INFILE
    ├── dbt_proyecto/            # pendiente
    └── prefect/                 # pendiente
```

---

## Modelo dimensional

**Pendiente de definir:** Kimball (estrella) — candidato principal sobre OBT por alta cardinalidad
de dimensiones (`dim_ubicacion` ~500 municipios, `dim_causa` ~40 causas).

Candidatos para dimensiones:
- `dim_fecha` — fecha, día semana, fase del día, mes, año
- `dim_ubicacion` — UF, municipio, BR (carretera), km, coordenadas
- `dim_causa` — causa, tipo de accidente, clasificación
- `dim_via` — tipo de pista, trazado, sentido, uso del suelo
- `dim_clima` — condición meteorológica PRF + variables Open-Meteo ERA5
- `fct_accidentes` — tabla de hechos con víctimas, vehículos, FK a dimensiones

---

## Estado general

| Componente | Estado | Detalle |
|---|---|---|
| Definición de fuentes de datos | ✅ Listo | DATATRAN 2026 + Open-Meteo ERA5 |
| Pre-procesamiento CSV | ✅ Listo | `convert_csv_to_utf8.py` con `.env`, salida a `data/` |
| Infraestructura Docker | ✅ Listo | `tf-mysql`, `tf-phpmyadmin`, `tf-metabase` operativos |
| Carga MySQL | ✅ Listo | 11.380 registros en `datatran.accidentes_raw` verificados |
| Metabase — conexión MotherDuck | ✅ Listo | `md:airbyte_trabajo`, driver DuckDB configurado |
| Reporte técnico (Typst) | 🔄 En progreso | Secciones 1–6 redactadas, pendiente resultados finales |
| Definición del modelo dimensional | 🔄 En progreso | Candidatos definidos, pendiente implementación |
| Airbyte connections | ⏳ Pendiente | — |
| dbt modelos staging | ⏳ Pendiente | — |
| dbt modelos marts | ⏳ Pendiente | — |
| dbt tests (dbt-expectations) | ⏳ Pendiente | — |
| Prefect pipeline | ⏳ Pendiente | — |
| Metabase dashboard | ⏳ Pendiente | Conexión lista, faltan visualizaciones |
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
| Al menos 2 fuentes de datos configuradas correctamente | 5 | ⏳ Pendiente |
| Conexión al Data Warehouse (MotherDuck) funcionando | 4 | ⏳ Pendiente |
| Todas las tablas necesarias sincronizadas | 4 | ⏳ Pendiente |
| Sync mode apropiado para el caso de uso | 2 | ⏳ Pendiente |

- [ ] Source MySQL (`datatran.accidentes_raw`) configurado en Airbyte
- [ ] Source Open-Meteo ERA5 configurado (script de extracción o Custom Connector)
- [ ] Destination MotherDuck (`md:airbyte_trabajo`) configurado
- [ ] Connections creadas y sync completado
- [ ] Sync mode elegido y justificado (Full Refresh para dataset estático 2026)

### 2. Modelado y Transformación con dbt — 25 pts

| Criterio | Pts | Estado |
|---|---|---|
| Proyecto dbt estructurado correctamente (staging/, marts/) | 3 | ⏳ Pendiente |
| Sources definidos en YAML con database y schema | 2 | ⏳ Pendiente |
| Modelos staging con limpieza y tipado de datos | 5 | ⏳ Pendiente |
| Modelo dimensional (Kimball) o OBT implementado | 6 | ⏳ Pendiente |
| Justificación del enfoque de modelado elegido | 4 | ⏳ Pendiente |
| Uso correcto de `ref()` y `source()` para dependencias | 2 | ⏳ Pendiente |
| Materializations apropiados (view/table/incremental) | 3 | ⏳ Pendiente |

- [ ] Proyecto dbt inicializado, `profiles.yml` apuntando a `md:airbyte_trabajo`
- [ ] `sources.yml` con database y schema correctos
- [ ] `stg_accidentes`: `NULLIF`, `REPLACE` comas decimales, `CAST` de tipos
- [ ] `stg_clima_openmeteo`: tipado directo, decodificación WMO
- [ ] Modelo dimensional (estrella Kimball) implementado
- [ ] Materializations definidos: `view` en staging, `table` en marts
- [ ] `dbt run` sin errores

### 3. Calidad de Datos con Testing — 15 pts

| Criterio | Pts | Estado |
|---|---|---|
| Tests `unique` y `not_null` en primary keys | 3 | ⏳ Pendiente |
| Mínimo 5 tests de `dbt-expectations` implementados | 6 | ⏳ Pendiente |
| Tests cubren diferentes dimensiones de calidad | 3 | ⏳ Pendiente |
| Todos los tests pasan exitosamente | 3 | ⏳ Pendiente |

- [ ] `unique` + `not_null` en `fct_accidentes.id`
- [ ] `expect_column_values_to_be_between` en `mortos` (0–100) y `latitude` (-34 a 6)
- [ ] `expect_column_values_to_not_be_null` en `uf`
- [ ] `expect_column_proportion_of_unique_values` en `municipio`
- [ ] `expect_column_values_to_match_regex` en `data_inversa`
- [ ] `dbt test` sin errores

### 4. Orquestación con Prefect — 12 pts

| Criterio | Pts | Estado |
|---|---|---|
| Pipeline definido con decoradores `@flow` y `@task` | 3 | ⏳ Pendiente |
| Integración con Airbyte (API o SDK) | 4 | ⏳ Pendiente |
| Integración con dbt (`prefect-dbt`) | 3 | ⏳ Pendiente |
| Manejo de errores y logging apropiado | 2 | ⏳ Pendiente |

- [ ] Flow `datatran_pipeline` con tasks: `extract_and_load` + `transform_with_dbt` + `test_with_dbt`
- [ ] Task Airbyte: polling via API REST hasta completar sync
- [ ] Task dbt: usando `prefect-dbt`
- [ ] Try/except con logging en cada task
- [ ] Ejecución exitosa en Prefect UI (captura de pantalla)

### 5. Visualización con Metabase — 15 pts

| Criterio | Pts | Estado |
|---|---|---|
| Conexión al Data Warehouse funcionando | 2 | ✅ Listo |
| Mínimo 5 visualizaciones implementadas | 5 | ⏳ Pendiente |
| Visualizaciones responden preguntas de negocio claras | 4 | ⏳ Pendiente |
| Dashboard organizado con filtros interactivos | 4 | ⏳ Pendiente |

- [x] Conexión a `md:airbyte_trabajo` configurada (`Motherduck_Trabajo_Final`)
- [ ] Visualización 1: mapa de calor — accidentes mortales por estado/ruta
- [ ] Visualización 2: línea temporal — evolución mensual de accidentes
- [ ] Visualización 3: barra apilada — causas bajo lluvia vs cielo despejado
- [ ] Visualización 4: scatter — precipitación ERA5 vs mortalidad
- [ ] Visualización 5: tabla pivot — condición PRF vs código WMO ERA5
- [ ] Filtros: período de fecha, UF, causa del accidente
- [ ] Captura del dashboard

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
| 1. Extracción (Airbyte) | 15 | — |
| 2. Modelado y Transformación (dbt) | 25 | — |
| 3. Calidad de Datos (Testing) | 15 | — |
| 4. Orquestación (Prefect) | 12 | — |
| 5. Visualización (Metabase) | 15 | 2 (conexión ✅) |
| 6. Reporte Técnico | 10 | — |
| 7. Video Explicativo | 8 | — |
| **TOTAL** | **100** | **2** |

---

## Pasos siguientes (orden sugerido)

### 1. Airbyte — configurar las dos fuentes y el destination

El MySQL ya está operativo con los datos cargados. Configurar en Airbyte:

- **Source MySQL:** host con IP accesible desde Airbyte, base `datatran`, tabla `accidentes_raw`,
  usuario `airbyte`, sync mode Full Refresh
- **Source Open-Meteo ERA5:** definir estrategia (Custom Connector o script de extracción que
  genere un CSV/JSON y se cargue como File source)
- **Destination MotherDuck:** database `md:airbyte_trabajo`, schema `datatran_raw`
- Ejecutar sync y verificar tablas en MotherDuck

### 2. dbt — inicializar proyecto y modelos staging

```bash
cd workspaces/dbt_proyecto
dbt init
# Configurar profiles.yml con md:airbyte_trabajo y MOTHERDUCK_TOKEN
dbt debug   # verificar conexión
```

Modelos a crear en orden:
1. `models/staging/sources.yml` — declarar fuentes con database/schema de Airbyte
2. `models/staging/stg_accidentes.sql` — tipado y limpieza de `accidentes_raw`
3. `models/staging/stg_clima_openmeteo.sql` — tipado de datos ERA5
4. `models/marts/dim_*.sql` — dimensiones del esquema estrella
5. `models/marts/fct_accidentes.sql` — tabla de hechos

### 3. dbt — tests de calidad

Agregar en `models/staging/schema.yml`:
- Tests genéricos (`unique`, `not_null`) en PKs
- Al menos 5 tests de `dbt-expectations` cubriendo validez, completitud y consistencia

```bash
dbt test
```

### 4. Prefect — pipeline de orquestación

Adaptar `Tarea_Clase_5/workspaces/maven-fuzzy/prefect/ecommerce_pipeline.py`:
- Cambiar connection ID de Airbyte por el del nuevo sync
- Cambiar rutas del proyecto dbt
- Verificar ejecución en `http://localhost:4200`

### 5. Metabase — dashboard

Con los marts de dbt disponibles en MotherDuck:
- Crear las 5 visualizaciones definidas en el checklist
- Configurar filtros de fecha, UF y condición climática
- Tomar captura del dashboard completo

### 6. Reporte técnico — completar resultados

- Agregar capturas de: Airbyte sync exitoso, `dbt run`, `dbt test`, Prefect UI, dashboard
- Completar sección de resultados en `main.typ`
- Exportar a PDF: `typst compile docs/typst/main.typ`

### 7. Video

Grabar demostración end-to-end del pipeline una vez que todos los pasos anteriores estén completos.

---

## Reutilización de Tarea 7

Los siguientes componentes se pueden copiar/adaptar de
`Tarea_Clase_5/workspaces/maven-fuzzy/`:

| Componente | Archivo origen | Cambios necesarios |
|---|---|---|
| Docker Metabase | `containers/Dockerfile` | Ninguno (ya copiado) |
| docker-compose | `containers/docker-compose.yaml` | Aplicado — prefijo `tf-` |
| dbt profiles | `dbt_maven_fuzzy/profiles.yml` | Cambiar nombre de proyecto y schema |
| dbt project | `dbt_maven_fuzzy/dbt_project.yml` | Renombrar a `dbt_datatran` |
| Pipeline Prefect | `prefect/ecommerce_pipeline.py` | Ajustar connection ID y rutas dbt |
