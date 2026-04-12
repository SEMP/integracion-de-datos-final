#import "@preview/basic-report:0.4.0": *

// Renderiza un identificador de código con oportunidades de salto de línea
// después de cada guión bajo. Útil para nombres largos de tests dbt en tablas.
#let dbt(s) = {
  let parts = s.split("_")
  parts.enumerate().map(((i, p)) => {
    if i < parts.len() - 1 { [#raw(p)#raw("_")#sym.zws] }
    else { raw(p) }
  }).join()
}

#show: it => basic-report(
  doc-category: "Integración de datos",
  doc-title: "Trabajo Práctico Final",
  author: "Sergio Enrique Morel Peralta\nClara Patricia Almirón Burgos\nDaniel Ramírez",
  affiliation: "Facultad Politécnica - UNA",
  logo: image("assets/fpuna_logo_institucional.svg", width: 2cm),
  language: "es",
  compact-mode: true,
  heading-font: "Lato",
  it
)
#v(-8em)
#align(center)[
  #image("assets/fpuna_logo_institucional.svg", width: 3cm)
]

= Pipeline ELT: Análisis de Accidentes de Tráfico en Brasil

Este trabajo implementa un pipeline ELT completo que integra datos de accidentes de tráfico registrados por la Policía Rodoviária Federal de Brasil (DATATRAN/PRF) con datos climáticos históricos de la API Open-Meteo ERA5. El objetivo es construir un modelo dimensional analítico que permita explorar la relación entre condiciones climáticas y accidentalidad vial en rutas federales brasileñas durante 2026.

La arquitectura del pipeline sigue la secuencia: *MySQL → Airbyte → MotherDuck → dbt → Metabase*, orquestada con *Prefect*.

== Introducción

=== Problemática y motivación

Los accidentes de tráfico representan una de las principales causas de mortalidad en Brasil. La Policía Rodoviária Federal registra sistemáticamente cada accidente ocurrido en rutas federales, incluyendo coordenadas GPS, condiciones meteorológicas según el agente, y datos de víctimas. Sin embargo, la condición climática registrada por el agente (`condicao_metereologica`) es subjetiva y limitada. Enriquecer este dataset con datos climáticos objetivos —precipitación real, temperatura, cobertura de nubes— permite análisis más precisos de la influencia del clima en la accidentalidad.

=== Fuentes de datos

==== Fuente 1: DATATRAN PRF 2026

El dataset proviene del portal de datos abiertos del gobierno brasileño #cite(<dados_gov>) y contiene todos los accidentes registrados por la PRF en rutas federales durante el año 2026.

#table(
  columns: (auto, 1fr),
  table.header([*Característica*], [*Valor*]),
  [Registros],     [11.380 accidentes],
  [Período],       [Enero – Abril 2026],
  [Encoding],      [ISO-8859-1 (Latin-1)],
  [Separador],     [Punto y coma (`;`)],
  [Fin de línea],  [CRLF (Windows)],
  [Coordenadas],   [Latitud y longitud GPS por accidente],
  [Decimales],     [Coma como separador (ej: `-7,291548`)],
)

Las 30 columnas del dataset cubren localización (UF, municipio, BR, km, lat/lon), causa y tipo de accidente, condición meteorológica según el agente PRF, fase del día, características de la vía, y conteos de víctimas (muertos, heridos leves, heridos graves, ilesos).

Un campo de atención especial es `tracado_via`, que puede contener múltiples valores separados por punto y coma dentro del mismo campo (ej: `"Aclive;Reta"`), lo que requiere que las herramientas de carga respeten el entrecomillado del CSV.

==== Fuente 2: Open-Meteo ERA5 (clima histórico)

Open-Meteo #cite(<openmeteo>) provee datos de reanálisis ERA5 con resolución horaria para cualquier coordenada geográfica. El join con DATATRAN se realiza mediante `data_inversa` + `horario` + `latitude` + `longitude` de cada accidente.

#table(
  columns: (auto, 1fr),
  table.header([*Variable ERA5*], [*Relevancia vial*]),
  [`precipitation`],       [★★★ — Precipitación mm/h],
  [`weather_code` (WMO)],  [★★★ — Condición climática estandarizada],
  [`temperature_2m`],      [★★ — Temperatura ambiente],
  [`relative_humidity_2m`],[★★ — Humedad relativa],
  [`dew_point_2m`],        [★★ — Punto de rocío (riesgo de escarcha)],
  [`is_day`],              [★★ — Flag día/noche],
  [`cloud_cover_low`],     [★ — Nubes bajas (proxy de niebla)],
  [`wind_speed_10m`],      [★ — Velocidad del viento],
  [`wind_gusts_10m`],      [★ — Ráfagas de viento],
  [`shortwave_radiation`], [★ — Radiación solar (encandilamiento)],
)

==== Decisión: Open-Meteo ERA5 vs OpenWeather One Call 3.0

Se evaluaron dos APIs para datos climáticos históricos. Ambas se probaron contra 5 accidentes reales del dataset, obteniéndose 5/5 respuestas exitosas en las dos. La tabla siguiente resume los factores determinantes:

#table(
  columns: (1fr, auto, auto),
  table.header([*Criterio*], [*Open-Meteo ERA5*], [*OpenWeather 3.0*]),
  [Costo],              [Gratuito, sin API key], [Suscripción paga],
  [`visibility`],       [No disponible],         [No disponible],
  [`wind_gusts`],       [Disponible],            [`None` en todos los casos],
  [Nubes bajas],        [`cloud_cover_low`],     [No disponible],
  [Radiación solar],    [`shortwave_radiation`], [No disponible],
  [Fase del día],       [Flag `is_day`],         [Calculado de sunrise/sunset],
  [Clasificación WMO],  [Estándar internacional],[Etiquetas propias],
)

Se eligió *Open-Meteo ERA5* por ser gratuito y proveer mayor cantidad de variables útiles. La visibilidad (`visibility`) no está disponible en ninguna de las dos APIs para datos ERA5 históricos; se usa `condicao_metereologica` de la PRF como proxy.

== Paso 1: Pre-procesamiento del CSV

Antes de levantar MySQL, el CSV original debe convertirse a UTF-8. Esta conversión es necesaria porque:

- MySQL 8.0 con `utf8mb4` rechaza datos Latin-1 si el cliente no indica el charset correcto.
- Los campos con acentos (`Céu Claro`, `carroçável`) quedan corruptos si se carga el archivo sin conversión explícita.
- Normalizar CRLF a LF simplifica el `LOAD DATA INFILE`.

=== Script de conversión

El script `workspaces/scripts/convert_csv_to_utf8.py` realiza la conversión usando únicamente la biblioteca estándar de Python:

```python
import csv

with (
    open(input_path, encoding="latin-1", newline="") as src,
    open(output_path, "w", encoding="utf-8", newline="\n") as dst,
):
    reader = csv.reader(src, delimiter=";", quotechar='"')
    writer = csv.writer(dst, delimiter=";", quotechar='"',
                        quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    for row in reader:
        writer.writerow([field.strip() for field in row])
```

El uso del módulo `csv` con `quotechar='"'` es crítico: garantiza que el campo `tracado_via` —que contiene `;` internos como `"Aclive;Reta"`— sea leído y reescrito como un único campo entrecomillado, sin romper la estructura del CSV.

=== Ejecución

```bash
python3 workspaces/scripts/convert_csv_to_utf8.py
# Salida esperada:
# Leyendo  : .../datatran2026.csv
# Escribiendo: .../datatran2026_utf8.csv
# Filas escritas (incluye encabezado): 11381
```

El archivo resultante `datatran2026_utf8.csv` queda en el mismo directorio que el original y es el que se monta en el contenedor MySQL.

== Paso 2: Base de datos MySQL con Docker

=== Contenedor Docker

Se levanta MySQL 8.0 junto a phpMyAdmin y Metabase mediante `docker-compose.yaml` en `workspaces/containers/`. Las credenciales se leen desde un archivo `.env` (no versionado); el archivo `example.env` sirve de plantilla.

```yaml
services:
  mia-mysql:
    image: mysql:8.0
    command: >
      --default-authentication-plugin=mysql_native_password
      --local-infile=1
      --secure-file-priv=/csv
    volumes:
      - ./initdb:/docker-entrypoint-initdb.d:ro
      - ${CSV_DIR}:/csv
```

La opción `--default-authentication-plugin=mysql_native_password` garantiza compatibilidad con el conector de Airbyte. La directiva `--secure-file-priv=/csv` restringe `LOAD DATA INFILE` al directorio `/csv`, donde se monta `${CSV_DIR}` (la carpeta local con el CSV convertido).

=== Schema: todo TEXT

La tabla `accidentes_raw` define todos sus campos como `TEXT`. Esta decisión evita cualquier fallo de conversión de tipos durante la ingesta y delega la lógica de tipado a dbt, donde es versionable y testeable:

#table(
  columns: (auto, auto, 1fr),
  table.header([*Campo*], [*Tipo MySQL*], [*Motivo*]),
  [`km`, `latitude`, `longitude`], [TEXT], [Decimal con coma: `-7,291548`],
  [`classificacao_acidente`],      [TEXT], [Contiene el valor literal `NA`],
  [`tracado_via`],                 [TEXT], [Valores múltiples con `;` interno],
  [`data_inversa`],                [TEXT], [String — dbt castea a DATE],
  [`horario`],                     [TEXT], [String — dbt castea a TIME],
  [`id`, `mortos`, etc.],          [TEXT], [Enteros — dbt castea a INT/BIGINT],
)

```sql
CREATE DATABASE IF NOT EXISTS datatran
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE datatran;

CREATE TABLE accidentes_raw (
    id TEXT, data_inversa TEXT, dia_semana TEXT,
    horario TEXT, uf TEXT, br TEXT, km TEXT,
    municipio TEXT, causa_acidente TEXT, tipo_acidente TEXT,
    classificacao_acidente TEXT, fase_dia TEXT, sentido_via TEXT,
    condicao_metereologica TEXT, tipo_pista TEXT, tracado_via TEXT,
    uso_solo TEXT, pessoas TEXT, mortos TEXT, feridos_leves TEXT,
    feridos_graves TEXT, ilesos TEXT, ignorados TEXT, feridos TEXT,
    veiculos TEXT, latitude TEXT, longitude TEXT,
    regional TEXT, delegacia TEXT, uop TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

=== Carga con LOAD DATA INFILE

```sql
LOAD DATA INFILE '/csv/datatran2026_utf8.csv'
INTO TABLE accidentes_raw
CHARACTER SET utf8mb4
FIELDS
    TERMINATED BY ';'
    ENCLOSED BY '"'
LINES
    TERMINATED BY '\n'
IGNORE 1 LINES;
```

`ENCLOSED BY '"'` es la cláusula crítica: sin ella, los valores de `tracado_via` que contienen `;` interno (como `"Aclive;Reta"`) se interpretarían como dos campos separados, corrompiendo todas las columnas a partir de esa posición.

=== Verificación

```bash
docker exec -it mia-mysql mysql -u airbyte -pairbyte datatran \
  -e "SELECT COUNT(*) AS total FROM accidentes_raw;"
-- Resultado esperado: 11380

docker exec -it mia-mysql mysql -u airbyte -pairbyte datatran \
  -e "SELECT tracado_via FROM accidentes_raw
      WHERE tracado_via LIKE '%;%' LIMIT 3;"
-- Resultado: Aclive;Reta | Declive;Curva | Reta;Em Obras
```

== Paso 3: Configurar Airbyte

=== Source: MySQL (DATATRAN)

#table(
  columns: (auto, 1fr),
  table.header([*Campo*], [*Valor*]),
  [Nombre],        [`MySQL_DATATRAN_2026`],
  [Host],          [IP de la máquina con Docker],
  [Port],          [`3306`],
  [Base de datos], [`datatran`],
  [Usuario],       [`airbyte`],
  [Tabla],         [`accidentes_raw`],
  [Sync mode],     [Full Refresh — dataset estático de 2026],
)

=== Source: Open-Meteo ERA5 (clima)

#table(
  columns: (auto, 1fr),
  table.header([*Campo*], [*Valor*]),
  [Tipo],          [Custom connector / script de extracción],
  [Endpoint],      [`https://archive-api.open-meteo.com/v1/archive`],
  [Parámetros],    [`latitude`, `longitude`, `start_date`, `end_date`, `hourly`],
  [Sin API key],   [Gratuito],
)

=== Destination: MotherDuck

#table(
  columns: (auto, 1fr),
  table.header([*Campo*], [*Valor*]),
  [Nombre],         [`MotherDuck_datatran`],
  [Database],       [`md:airbyte_curso`],
  [Default schema], [`datatran_raw`],
)

== Paso 4: Modelos dbt

El proyecto dbt en `workspaces/dbt_proyecto/` transforma los datos crudos en dos capas.

=== Configuración

```yaml
# profiles.yml
dbt_datatran:
  outputs:
    dev:
      type: duckdb
      path: "md:airbyte_curso"
      schema: datatran
      motherduck_token: "{{ env_var('MOTHERDUCK_TOKEN') }}"
  target: dev
```

```yaml
# dbt_project.yml
models:
  dbt_datatran:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
```

=== Modelos staging

Los modelos staging aplican las transformaciones de tipo que no se pudieron hacer en MySQL:

#table(
  columns: (auto, 1fr),
  table.header([*Modelo*], [*Transformaciones principales*]),
  [`stg_accidentes`],
  [
    `NULLIF(campo, 'NA')` para nulos literales;\
    `REPLACE(latitude, ',', '.')::DOUBLE`;\
    `REPLACE(km, ',', '.')::DECIMAL`;\
    `CAST(id AS BIGINT)`, `CAST(mortos AS INT)` y demás enteros;\
    `CAST(data_inversa AS DATE)`, `CAST(horario AS TIME)`
  ],
  [`stg_clima_openmeteo`],
  [
    Tipado directo (ERA5 ya entrega datos numéricos);\
    Decodificación de `weather_code` WMO a descripción legible;\
    Join key: `accidente_id` (lat + lon + fecha + hora)
  ],
)

=== Modelo dimensional

Se implementa un esquema en estrella (Kimball) con las siguientes entidades:

#table(
  columns: (auto, 1fr),
  table.header([*Tabla*], [*Descripción*]),
  [`dim_fecha`],     [Fecha, día de la semana, fase del día, mes, año],
  [`dim_ubicacion`], [UF, municipio, BR, km, latitud, longitud],
  [`dim_causa`],     [Causa del accidente, tipo, clasificación],
  [`dim_via`],       [Tipo de pista, trazado, sentido, uso del suelo],
  [`dim_clima`],     [Condición PRF + variables ERA5: precipitación, temperatura, código WMO],
  [`fct_accidentes`],[Tabla de hechos: víctimas, vehículos, FK a todas las dimensiones],
)

La elección de Kimball sobre OBT se justifica por la presencia de dimensiones de alta cardinalidad (`dim_ubicacion` con ~500 municipios, `dim_causa` con ~40 causas distintas) que se benefician de la normalización para evitar redundancia y facilitar el filtrado en Metabase.

=== Tests de calidad (dbt-expectations)

#table(
  columns: (2fr, 2fr, 1fr),
  table.header([*Test*], [*Modelo*], [*Dimensión de calidad*]),
  [#dbt("unique") + #dbt("not_null")],                       [#dbt("fct_accidentes.id")],          [Unicidad / completitud],
  [#dbt("expect_column_values_to_be_between")],              [#dbt("stg_accidentes.mortos")],      [Validez: rango 0–100],
  [#dbt("expect_column_values_to_be_between")],              [#dbt("stg_accidentes.latitude")],    [Validez: rango -34 a 6],
  [#dbt("expect_column_values_to_not_be_null")],             [#dbt("stg_accidentes.uf")],          [Completitud],
  [#dbt("expect_column_proportion_of_unique_values")],       [#dbt("stg_accidentes.municipio")],   [Consistencia: >100 municipios],
  [#dbt("expect_column_values_to_match_regex")],             [#dbt("stg_accidentes.data_inversa")],[Formato YYYY-MM-DD],
)

== Paso 5: Orquestación con Prefect

El pipeline se orquesta con Prefect 3, coordinando las tres etapas en secuencia:

=== Estructura del pipeline

```python
@flow(name="datatran_pipeline")
def datatran_pipeline():
    extract_and_load()   # Dispara sync Airbyte vía API REST
    transform_with_dbt() # dbt deps + dbt run
    test_with_dbt()      # dbt test
```

#table(
  columns: (auto, auto, 1fr),
  table.header([*Task*], [*Nombre*], [*Descripción*]),
  [1], [`Extract and Load`],   [Dispara el sync de Airbyte vía API REST y hace polling cada 10s hasta completar. Reintentos: 2],
  [2], [`Transform with dbt`], [Ejecuta `dbt deps` + `dbt run` como subproceso con `prefect-dbt`],
  [3], [`Test with dbt`],      [Ejecuta `dbt test` y falla el flow si algún test no pasa],
)

== Paso 6: Visualización con Metabase

Metabase se levanta como contenedor Docker desde la misma imagen personalizada usada en las tareas anteriores, que combina Metabase v0.58.8 con el driver DuckDB de MotherDuck:

```dockerfile
FROM eclipse-temurin:21-jre
ADD https://downloads.metabase.com/v0.58.8/metabase.jar /app/metabase.jar
ADD https://github.com/motherduckdb/metabase_duckdb_driver/releases/\
download/1.4.3.1/duckdb.metabase-driver.jar /plugins/
```

=== Conexión a MotherDuck

En el setup inicial de Metabase se selecciona el driver *DuckDB* y se configura:

#table(
  columns: (auto, 1fr),
  table.header([*Campo*], [*Valor*]),
  [Database file],         [`md:airbyte_curso`],
  [MotherDuck Token],      [Token de la cuenta (campo separado del driver)],
  [Old implicit casting],  [Activado],
)

=== Visualizaciones planeadas

#table(
  columns: (auto, auto, 1fr),
  table.header([*\#*], [*Tipo*], [*Pregunta de negocio*]),
  [1], [Mapa de calor],  [¿En qué estados y rutas se concentran los accidentes mortales?],
  [2], [Línea temporal], [¿Cómo evoluciona la cantidad de accidentes por mes?],
  [3], [Barra apilada],  [¿Qué causas predominan bajo precipitación vs cielo despejado?],
  [4], [Scatter plot],   [¿Existe correlación entre precipitación (mm/h) y mortalidad?],
  [5], [Tabla pivot],    [Mortalidad promedio por condición PRF vs código WMO ERA5],
)

Filtros configurados: período de fecha, UF, causa del accidente, condición climática ERA5.

== Conclusiones

El pipeline ELT implementado integra dos fuentes de datos heterogéneas —accidentes de tráfico en formato CSV con problemas de encoding y campos compuestos, y datos climáticos históricos de una API REST gratuita— en un modelo dimensional analítico en MotherDuck.

Las decisiones técnicas más relevantes fueron:

- *Encoding y pre-procesamiento fuera del contenedor:* la conversión Latin-1 → UTF-8 se realiza con un script Python antes de levantar MySQL, manteniendo la ingesta SQL simple y sin dependencias externas en el contenedor.

- *Todo TEXT en la capa raw:* evita que MySQL rechace o trunque datos con formatos no estándar (decimales con coma, nulos literales `NA`, fechas como string). dbt es el único responsable del tipado, lo que hace la transformación auditable y testeable.

- *Open-Meteo ERA5 sobre OpenWeather:* la API gratuita provee más variables relevantes para el análisis vial (ráfagas, nubes bajas, radiación solar) y no requiere gestión de claves API, simplificando la configuración y el despliegue.

- *Esquema estrella (Kimball):* la normalización en dimensiones facilita los filtros en Metabase y evita la redundancia que tendría una OBT con municipios y causas repetidos en cada fila de hecho.

#bibliography("refs.bib", title: "Referencias", style: "ieee")
