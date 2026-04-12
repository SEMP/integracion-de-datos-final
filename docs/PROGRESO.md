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
- **Archivo disponible:** `/home/sergio/Documents/Facultad/Maestria/09-MIA_3_Introduccion_a_la_integracion_de_datos/workspaces/data/datatran2026.csv`
  - ~11,380 registros, año 2026
- **Referencia histórica:** SQLite con datos 2011-2016 (~2.18M registros) en
  `/home/sergio/Documents/Facultad/Maestria/07-MIA_10-Analisis_de_Datos_con_Metodos_de_Data_Driven/workspaces/tp1_data_driven/data/datatran_raw.db`
- **Columnas clave:**
  - `id`, `data_inversa` (fecha), `horario`, `uf` (estado), `municipio`
  - `causa_acidente`, `tipo_acidente`, `classificacao_acidente`
  - `fase_dia`, `condicao_metereologica` (puente con clima)
  - `mortos`, `feridos_leves`, `feridos_graves`, `ilesos`, `veiculos`
  - `latitude`, `longitude`
- **Nota de carga:** separador `;`, encoding Latin1 — especificar en `LOAD DATA INFILE`

### Fuente 2: Clima histórico (OpenWeather API)
- **Origen:** OpenWeather Historical Weather API
- **Propósito:** Enriquecer accidentes con datos reales de clima (temperatura, lluvia, viento, visibilidad)
- **Puente con fuente 1:** fecha + coordenadas GPS (latitude/longitude) o municipio/UF
- **Pendiente:** definir estrategia de extracción (por fecha y ubicación de cada accidente)

---

## Estructura del workspace

```
workspaces/
├── containers/
│   ├── Dockerfile              # Metabase + driver DuckDB
│   ├── docker-compose.yaml     # MySQL + phpMyAdmin + Metabase
│   ├── example.env
│   └── initdb/
│       ├── 01_schema.sql
│       ├── 02_load_data.sql
│       └── 03_create_metabase_db.sh
├── dbt_proyecto/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── tests/
└── prefect/
    ├── ecommerce_pipeline.py
    └── .env.example
```

---

## Modelo dimensional

**Pendiente de definir:** Kimball (estrella/copo de nieve) vs OBT (One Big Table).

Candidatos para dimensiones:
- `dim_fecha` — fecha, día semana, fase del día
- `dim_ubicacion` — UF, municipio, BR (carretera), km, coordenadas
- `dim_causa` — causa, tipo de accidente, clasificación
- `dim_via` — tipo de pista, trazado, sentido, uso del suelo
- `dim_clima` — condición meteorológica (fuente PRF + datos OpenWeather)
- `fct_accidentes` — tabla de hechos con víctimas, vehículos, FK a dimensiones

---

## Checklist de entregables (Grilla de Evaluación)

### Infraestructura base (no evaluada directamente)
- [ ] `docker-compose.yaml` con MySQL 8.0 + phpMyAdmin + Metabase
- [ ] `initdb/01_schema.sql` — schema de la tabla de accidentes
- [ ] `initdb/02_load_data.sql` — carga CSV con `LOAD DATA INFILE` (sep `;`, Latin1)
- [ ] `initdb/03_create_metabase_db.sh` — crea DB y permisos para Metabase
- [ ] `example.env` con todas las variables requeridas
- [ ] `docker compose up -d` ejecutado y datos verificados

### 1. Extracción con Airbyte — 15 pts

| Criterio | Pts | Estado |
|---|---|---|
| Al menos 2 fuentes de datos configuradas correctamente | 5 | Pendiente |
| Conexión al Data Warehouse (MotherDuck) funcionando | 4 | Pendiente |
| Todas las tablas necesarias sincronizadas | 4 | Pendiente |
| Sync mode apropiado para el caso de uso | 2 | Pendiente |

- [ ] Source MySQL (accidentes DATATRAN) configurado en Airbyte
- [ ] Source OpenWeather (o carga directa) configurado en Airbyte
- [ ] Destination MotherDuck configurado
- [ ] Connections creadas y sync completado
- [ ] Sync mode elegido y justificado (full refresh vs incremental)

### 2. Modelado y Transformación con dbt — 25 pts

| Criterio | Pts | Estado |
|---|---|---|
| Proyecto dbt estructurado correctamente (staging/, marts/) | 3 | Pendiente |
| Sources definidos en YAML con database y schema | 2 | Pendiente |
| Modelos staging con limpieza y tipado de datos | 5 | Pendiente |
| Modelo dimensional (Kimball) o OBT implementado | 6 | Pendiente |
| Justificación del enfoque de modelado elegido | 4 | Pendiente |
| Uso correcto de `ref()` y `source()` para dependencias | 2 | Pendiente |
| Materializations apropiados (view/table/incremental) | 3 | Pendiente |

- [ ] Proyecto dbt inicializado, `profiles.yml` apuntando a MotherDuck
- [ ] `sources.yml` con database y schema correctos
- [ ] Modelos staging para accidentes (`stg_accidentes`) y clima (`stg_clima`)
- [ ] Modelo dimensional definido y justificado (Kimball vs OBT)
- [ ] Materializations elegidos y documentados
- [ ] `dbt run` sin errores

### 3. Calidad de Datos con Testing — 15 pts

| Criterio | Pts | Estado |
|---|---|---|
| Tests `unique` y `not_null` en primary keys | 3 | Pendiente |
| Mínimo 5 tests de `dbt-expectations` implementados | 6 | Pendiente |
| Tests cubren diferentes dimensiones de calidad | 3 | Pendiente |
| Todos los tests pasan exitosamente | 3 | Pendiente |

- [ ] Tests `unique` + `not_null` en todas las PKs
- [ ] Al menos 5 tests de `dbt-expectations` (ej: rangos de valores, formatos, distribuciones)
- [ ] Tests sobre diferentes dimensiones: completitud, unicidad, validez, consistencia
- [ ] `dbt test` sin errores

### 4. Orquestación con Prefect — 12 pts

| Criterio | Pts | Estado |
|---|---|---|
| Pipeline definido con decoradores `@flow` y `@task` | 3 | Pendiente |
| Integración con Airbyte (API o SDK) | 4 | Pendiente |
| Integración con dbt (`prefect-dbt`) | 3 | Pendiente |
| Manejo de errores y logging apropiado | 2 | Pendiente |

- [ ] Flow principal con tasks: `extract_and_load` + `transform` + `test_data`
- [ ] Task de Airbyte usando API o SDK de Airbyte
- [ ] Task de dbt usando `prefect-dbt`
- [ ] Try/except con logging en cada task
- [ ] Ejecución exitosa visible en Prefect UI (captura de pantalla)

### 5. Visualización con Metabase — 15 pts

| Criterio | Pts | Estado |
|---|---|---|
| Conexión al Data Warehouse funcionando | 2 | Pendiente |
| Mínimo 5 visualizaciones implementadas | 5 | Pendiente |
| Visualizaciones responden preguntas de negocio claras | 4 | Pendiente |
| Dashboard organizado con filtros interactivos | 4 | Pendiente |

- [ ] Conexión a MotherDuck configurada (campo Motherduck Token separado)
- [ ] Al menos 5 visualizaciones con preguntas de negocio definidas
- [ ] Filtros interactivos configurados (ej: por fecha, estado, tipo de accidente)
- [ ] Dashboard organizado y con título descriptivo
- [ ] Captura del dashboard

### 6. Reporte Técnico — 10 pts

| Criterio | Pts | Estado |
|---|---|---|
| Estructura correcta (intro, metodología, resultados, conclusión) | 3 | Pendiente |
| Documentación clara del pipeline y decisiones de diseño | 4 | Pendiente |
| Instrucciones de ejecución reproducibles | 3 | Pendiente |

- [ ] Secciones: introducción, metodología, resultados, conclusión
- [ ] Decisiones de diseño documentadas (elección de fuentes, modelo dimensional, materializations)
- [ ] Pasos para reproducir el pipeline (`docker compose up`, Airbyte, dbt, Prefect)
- [ ] Generado en Typst y exportado a PDF

### 7. Video Explicativo — 8 pts

| Criterio | Pts | Estado |
|---|---|---|
| Demostración del pipeline funcionando end-to-end | 4 | Pendiente |
| Explicación clara de la arquitectura y decisiones técnicas | 4 | Pendiente |

- [ ] Grabación mostrando el pipeline completo en ejecución
- [ ] Narración explicando arquitectura y decisiones técnicas

---

## Puntaje total: 100 pts

| Componente | Pts máx | Obtenido |
|---|---|---|
| 1. Extracción (Airbyte) | 15 | — |
| 2. Modelado y Transformación (dbt) | 25 | — |
| 3. Calidad de Datos (Testing) | 15 | — |
| 4. Orquestación (Prefect) | 12 | — |
| 5. Visualización (Metabase) | 15 | — |
| 6. Reporte Técnico | 10 | — |
| 7. Video Explicativo | 8 | — |
| **TOTAL** | **100** | **—** |

Escala: 5 = Excelente (90-100%) | 4 = Muy Bueno (75-89%) | 3 = Bueno (60-74%) | 2 = Regular (50-59%) | 1 = Insuficiente (<50%)

---

## Reutilización de Tarea 7

Los siguientes componentes se pueden copiar/adaptar de
`Tarea_Clase_5/workspaces/maven-fuzzy/`:

| Componente | Archivo origen | Cambios necesarios |
|---|---|---|
| Docker Metabase | `containers/Dockerfile` | Ninguno |
| docker-compose | `containers/docker-compose.yaml` | Renombrar servicios/volúmenes |
| initdb Metabase | `containers/initdb/03_create_metabase_db.sh` | Ninguno |
| dbt profiles | `dbt_maven_fuzzy/profiles.yml` | Cambiar nombre de proyecto y schema |
| dbt project | `dbt_maven_fuzzy/dbt_project.yml` | Renombrar proyecto |
| Pipeline Prefect | `prefect/ecommerce_pipeline.py` | Ajustar rutas y connection ID |
