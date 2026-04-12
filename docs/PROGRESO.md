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

## Checklist de entregables

### 0. Infraestructura Docker
- [ ] `docker-compose.yaml` con MySQL 8.0 + phpMyAdmin + Metabase
- [ ] `initdb/01_schema.sql` — schema de la tabla de accidentes
- [ ] `initdb/02_load_data.sql` — carga CSV con `LOAD DATA INFILE` (sep `;`, Latin1)
- [ ] `initdb/03_create_metabase_db.sh` — crea DB y permisos para Metabase
- [ ] `example.env` con todas las variables requeridas
- [ ] `docker compose up -d` ejecutado y datos verificados

### 1. Airbyte: Connections -> MotherDuck
- [ ] Source MySQL configurado (accidentes)
- [ ] Source para OpenWeather o carga directa configurada
- [ ] Destination MotherDuck configurado
- [ ] Connections creadas y sync completado

### 2. dbt: Modelos staging y marts
- [ ] Proyecto inicializado, `profiles.yml` configurado
- [ ] Modelos staging para accidentes y clima
- [ ] Modelo dimensional definido y justificado (Kimball vs OBT)
- [ ] Al menos 5 tests con `dbt-expectations`
- [ ] `dbt run` sin errores
- [ ] `dbt test` sin errores

### 3. Prefect: Orquestación
- [ ] Pipeline con tasks: extract_and_load + transform + test_data
- [ ] Ejecución exitosa en Prefect UI
- [ ] Captura de ejecución exitosa

### 4. Metabase: Dashboard
- [ ] Conexión a MotherDuck configurada (campo Motherduck Token separado)
- [ ] Al menos 5 visualizaciones
- [ ] Filtros configurados
- [ ] Captura del dashboard

### 5. Entregables finales
- [ ] Reporte técnico (Typst)
- [ ] Video presentación
- [ ] PDF del reporte

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

---

## Estado general

| Componente | Estado |
|---|---|
| Definición de fuentes de datos | Listo |
| Definición del modelo dimensional | Pendiente |
| Infraestructura Docker | Pendiente |
| Airbyte connections | Pendiente |
| dbt modelos staging | Pendiente |
| dbt modelos marts | Pendiente |
| dbt tests (dbt-expectations) | Pendiente |
| Prefect pipeline | Pendiente |
| Metabase dashboard | Pendiente |
| Reporte técnico | Pendiente |
| Video presentación | Pendiente |
