# Pipeline ELT — Accidentes de Tráfico en Brasil

Trabajo Práctico Final — Introducción a la Ingeniería de Datos (MIA 03, Febrero 2026)

## Descripción

Pipeline ELT completo que integra datos de accidentes de tráfico de Brasil (DATATRAN/PRF)
con datos climáticos históricos (Open-Meteo ERA5) para análisis multidimensional.

## Arquitectura

```
CSV DATATRAN 2026           Open-Meteo ERA5 API
(Latin-1, separador ;)      (gratuita, sin key)
         │                           │
         ▼                           │
 convert_csv_to_utf8.py              │
         │                           │
         ▼                           ▼
    MySQL 8.0              ┌─────────────────┐
  (Docker, todo TEXT)      │    Airbyte      │
         │                 │  (2 sources →   │
         └────────────────►│   MotherDuck)   │
                           └────────┬────────┘
                                    │
                                    ▼
                               MotherDuck
                             (Data Warehouse)
                                    │
                                    ▼
                              dbt (staging
                              + marts + tests)
                                    │
                          ┌─────────┴─────────┐
                          ▼                   ▼
                       Metabase           Prefect
                      (dashboard)      (orquestación)
```

## Fuentes de datos

### Fuente 1: DATATRAN PRF 2026
- **Origen:** Policía Rodoviária Federal de Brasil — [dados.gov.br](https://dados.gov.br)
- **Archivo:** `datatran2026.csv` (~11,380 accidentes, año 2026)
- **Formato:** CSV, separador `;`, encoding Latin-1, fin de línea CRLF
- **Nota:** el campo `tracado_via` contiene `;` como separador interno de valores
  (ej: `Aclive;Reta`) — está siempre entre comillas en el CSV

### Fuente 2: Open-Meteo ERA5 (clima histórico)
- **Origen:** [open-meteo.com](https://open-meteo.com) — reanalysis ERA5
- **Costo:** gratuito, sin API key
- **Join con fuente 1:** `data_inversa` + `horario` + `latitude` + `longitude`
- **Variables principales:** precipitación, temperatura, humedad, viento, condición WMO,
  cobertura de nubes bajas (proxy niebla), radiación solar, flag `is_day`

## Requisitos previos

- Docker y Docker Compose
- Python 3.8+  (solo biblioteca estándar, sin dependencias adicionales)
- Airbyte (instancia local o en la nube)
- Cuenta MotherDuck con token
- dbt Core con adaptador DuckDB (`dbt-duckdb`)

## Estructura del proyecto

```
Trabajo_Final/
├── README.md
├── docs/
│   ├── PROGRESO.md                          # Checklist y estado del proyecto
│   ├── Grilla_Evaluacion_TP_Final.xlsx      # Criterios de evaluación
│   └── Verificaciondesuscripcion_Extracciondedatos.ipynb  # Análisis fuentes clima
├── workspaces/
│   ├── scripts/
│   │   └── convert_csv_to_utf8.py           # Paso 1: conversión del CSV
│   ├── containers/
│   │   ├── Dockerfile                       # Metabase + driver DuckDB
│   │   ├── docker-compose.yaml              # MySQL + phpMyAdmin + Metabase
│   │   ├── example.env                      # Variables de entorno (plantilla)
│   │   └── initdb/
│   │       ├── 00_setup.sh                  # Crea BDs y otorga permisos (se ejecuta primero)
│   │       ├── 01_schema.sql                # Schema MySQL (todo TEXT)
│   │       └── 02_load_data.sql             # LOAD DATA INFILE
│   ├── dbt_proyecto/                        # (pendiente)
│   └── prefect/                             # (pendiente)
```

---

## Puesta en marcha

### Paso 1 — Convertir el CSV a UTF-8

El CSV original de DATATRAN está en Latin-1 con fin de línea CRLF. MySQL 8.0 requiere
UTF-8. El script `convert_csv_to_utf8.py` realiza la conversión sin modificar el archivo
original y maneja correctamente los campos con `;` internos (como `tracado_via`).

```bash
# 1. Copiar la plantilla y configurar la ruta a los CSV originales
cp workspaces/scripts/example.env workspaces/scripts/.env
# Editar .env y ajustar CSV_SOURCE_DIR con la ruta al directorio de los CSV originales

# 2. Ejecutar desde la raíz del proyecto
python3 workspaces/scripts/convert_csv_to_utf8.py
```

El script lee todos los `.csv` de `CSV_SOURCE_DIR` (definido en `workspaces/scripts/.env`)
y escribe los archivos convertidos en `data/` en la raíz del proyecto.

Salida esperada:
```
Origen  : /ruta/a/los/csvs/originales
Destino : /ruta/al/proyecto/data
Archivos: 1

  Leyendo    : .../datatran2026.csv
  Escribiendo: .../data/datatran2026_utf8.csv
  Filas escritas (incluye encabezado): 11381

Conversión completada.
```

Para convertir un archivo específico sin usar `.env`:
```bash
python3 workspaces/scripts/convert_csv_to_utf8.py \
    --input  /ruta/datatran2026.csv \
    --output /ruta/de/salida/datatran2026_utf8.csv
```

### Paso 2 — Configurar variables de entorno

```bash
cd workspaces/containers
cp example.env .env
```

Editar `.env` y ajustar al menos:

```dotenv
MYSQL_ROOT_PASSWORD=<contraseña segura>
MYSQL_PASSWORD=<contraseña para el usuario airbyte>
MYSQL_DATABASE=datatran
# CSV_DIR apunta a data/ en la raíz del proyecto (ruta relativa al docker-compose)
CSV_DIR=../../data
```

> `CSV_DIR` se monta en `/csv` dentro del contenedor MySQL. La ruta relativa
> `../../data` se resuelve desde `workspaces/containers/` hacia la raíz del proyecto.

### Paso 3 — Levantar los contenedores

```bash
cd workspaces/containers
docker compose up -d
```

Servicios que se inician:

| Servicio | Puerto | Descripción |
|---|---|---|
| `tf-mysql` | 3306 | MySQL 8.0 — fuente DATATRAN |
| `tf-phpmyadmin` | 8095 | Interfaz web para MySQL |
| `tf-metabase` | 3000 | Dashboard (Metabase + driver DuckDB) |

Al primer arranque, MySQL ejecuta automáticamente (en orden alfabético):
1. `00_setup.sh` — crea las bases `${MYSQL_DATABASE}` y `metabase`, otorga permisos a `airbyte`
2. `01_schema.sql` — crea la tabla `accidentes_raw` (todo TEXT)
3. `02_load_data.sql` — carga `datatran2026_utf8.csv` con `LOAD DATA INFILE`

### Paso 4 — Verificar la carga

```bash
docker exec -it tf-mysql mysql -u airbyte -pairbyte datatran \
    -e "SELECT COUNT(*) AS total FROM accidentes_raw;"
```

Resultado esperado: `11380`

```bash
# Verificar que tracado_via con ; internos se cargó correctamente
docker exec -it tf-mysql mysql -u airbyte -pairbyte datatran \
    -e "SELECT tracado_via FROM accidentes_raw WHERE tracado_via LIKE '%;%' LIMIT 5;"
```

### Paso 5 — Configurar Airbyte

1. **Source MySQL:**
   - Host: `localhost`, Puerto: `3306` (o el valor de `MYSQL_PORT`)
   - Base de datos: `datatran`
   - Usuario / Contraseña: los definidos en `.env`
   - Tabla a sincronizar: `accidentes_raw`
   - Sync mode: **Full Refresh** (dataset estático de 2026)

2. **Source Open-Meteo ERA5** (pendiente — script de extracción o Custom Connector)

3. **Destination MotherDuck:**
   - Token: el de tu cuenta MotherDuck
   - Database: `datatran_dw`

### Paso 6 — dbt: transformaciones y tests

```bash
cd workspaces/dbt_proyecto
dbt deps
dbt run
dbt test
```

Transformaciones que aplica dbt sobre `accidentes_raw`:
- `NULLIF(campo, 'NA')` para nulos literales
- `REPLACE(km, ',', '.')::DECIMAL` para decimales con coma
- `REPLACE(latitude, ',', '.')::DOUBLE` y `longitude`
- `CAST(id AS BIGINT)`, `CAST(mortos AS INT)`, etc.
- `CAST(data_inversa AS DATE)`, `CAST(horario AS TIME)`

### Paso 7 — Prefect: orquestación

```bash
cd workspaces/prefect
prefect server start &
python pipeline.py
```

### Paso 8 — Metabase: dashboard

Abrir `http://localhost:3000`, completar el setup inicial y agregar la base de datos:

| Campo | Valor |
|---|---|
| Driver | DuckDB |
| Nombre para mostrar | `Motherduck_Trabajo_Final` |
| Database file | `md:airbyte_trabajo` |
| Use DuckDB old_implicit_casting option | Activado |
| MotherDuck Token | Token de la cuenta (campo separado) |

---

## Detalle técnico: por qué se almacena todo como TEXT en MySQL

El CSV de DATATRAN tiene varios campos que requieren conversión no trivial:

| Campo | Problema | Solución en dbt |
|---|---|---|
| `km`, `latitude`, `longitude` | Decimal con coma (`-7,291548`) | `REPLACE(x, ',', '.')::DECIMAL` |
| `classificacao_acidente` | Valor `NA` literal como nulo | `NULLIF(x, 'NA')` |
| `tracado_via` | Múltiples valores con `;` interno | Se preserva como texto; split opcional en dbt |
| `data_inversa` | String `YYYY-MM-DD` | `CAST(x AS DATE)` |
| `horario` | String `HH:MM:SS` | `CAST(x AS TIME)` |
| `id`, `mortos`, etc. | Enteros como texto | `CAST(x AS BIGINT / INT)` |

Almacenar todo como TEXT en la tabla raw evita errores de tipo durante la ingesta y
delega toda la lógica de conversión a dbt, donde es testeable y versionable.

---

## Apagar los contenedores

```bash
cd workspaces/containers
docker compose down
# Para borrar también los datos persistidos:
docker compose down -v
```
