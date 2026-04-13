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
  doc-category: "Introducción a la Integración de Datos",
  doc-title: "Trabajo Práctico Final",
  author: "Sergio Enrique Morel Peralta\nClara Patricia Almirón de Silva\nDaniel Ramírez Brizuela",
  affiliation: "Facultad Politécnica - UNA",
  logo: image("assets/fpuna_logo_institucional.svg", width: 2cm),
  language: "es",
  compact-mode: true,
  heading-font: "Lato",
  datetime-fmt: "(2026-04-12)",
  it
)
#v(-8em)
#align(center)[
  #image("assets/fpuna_logo_institucional.svg", width: 3cm)
]

= Pipeline ELT: Análisis de Accidentes de Tráfico en Brasil

Este trabajo implementa un pipeline ELT completo que integra datos de accidentes de tráfico registrados por la Policía Rodoviária Federal de Brasil (DATATRAN/PRF) con datos climáticos históricos de la API Open-Meteo ERA5. El objetivo es construir un modelo dimensional analítico que permita explorar la relación entre condiciones climáticas y accidentalidad vial en rutas federales brasileñas durante 2026.

La arquitectura del pipeline sigue la secuencia: *MySQL → Airbyte → MotherDuck → dbt → Metabase*, orquestada con *Prefect*.

#figure(
  image("assets/arquitectura.svg", width: 100%),
  caption: [Arquitectura del pipeline ELT],
)

*Repositorio:* #link("https://github.com/SEMP/integracion-de-datos-final")

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
  [Nombre],         [`MySQL_Datatran`],
  [Host],           [`10.147.20.165` (IP ZeroTier — estable, no cambia)],
  [Port],           [`3306`],
  [Base de datos],  [`tf-datatran`],
  [Usuario],        [`airbyte`],
  [Encryption],     [required],
  [SSH Tunnel],     [No Tunnel],
  [Update Method],  [Scan Changes with User Defined Cursor],
)

#figure(
  image("assets/airbyte_source_mysql.png", width: 72%),
  caption: [Configuración del source MySQL en Airbyte],
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
  [Destination DB], [`md:airbyte_trabajo`],
  [Schema Name],    [`datatran`],
)

#figure(
  image("assets/airbyte_destination_motherduck.png", width: 72%),
  caption: [Configuración del destination MotherDuck en Airbyte],
)

=== Connection: sync mode

Se seleccionó *Full Refresh | Overwrite* para la tabla `accidentes_raw`. El dataset DATATRAN 2026 es estático (no recibe actualizaciones incrementales) y no dispone de columnas confiables de timestamp de modificación (`created_at`, `updated_at`) que habilitarían un modo incremental. La estrategia Overwrite garantiza que cada sync reemplaza completamente el contenido de la tabla destino, manteniendo consistencia sin riesgo de duplicados.

#figure(
  image("assets/airbyte_connection_datatran.png", width: 100%),
  caption: [Selección de sync mode: Full Refresh | Overwrite sobre `accidentes_raw` (30/30 campos)],
)

El paso siguiente configura los metadatos de la connection: nombre, tipo de schedule y namespace de destino. Se eligió *Destination-defined* como namespace para que los datos queden en el schema `datatran` configurado en el destination, y *Manual* como schedule dado que la orquestación la maneja Prefect.

#figure(
  image("assets/airbyte_connection_datatran_2.png", width: 100%),
  caption: [Configuración de la connection: `MySQL_Datatran → MotherDuck_datatran`, schedule Manual, namespace Destination-defined],
)

=== Incorporación de clima_raw como segundo stream

Una vez generado `clima_openmeteo.csv` y cargado en MySQL mediante el pipeline Prefect, se actualizó el schema de la connection para incluir `clima_raw` como segundo stream. Ambas tablas se sincronizan con *Full Refresh | Overwrite*: `accidentes_raw` con 30/30 campos y `clima_raw` con 13/13 campos.

#figure(
  image("assets/airbyte_connection_datatran_update.png", width: 100%),
  caption: [Schema actualizado: `accidentes_raw` (30 campos) y `clima_raw` (13 campos) — ambas con Full Refresh | Overwrite],
)

=== Resultado del sync

El primer sync completó exitosamente en 2 minutos 5 segundos, transfiriendo 7,39 MB con los 11.380 registros de `accidentes_raw`.

#figure(
  image("assets/airbyte_succesfull_sync_datatran.png", width: 100%),
  caption: [Timeline de Airbyte: sync exitoso — 11.380 registros cargados en MotherDuck],
)

La verificación en MotherDuck confirma que los datos quedaron disponibles en `datatran.accidentes_raw` dentro de la base `airbyte_trabajo`:

#figure(
  image("assets/motherduck_datatran_data.png", width: 100%),
  caption: [Vista previa de `datatran.accidentes_raw` en MotherDuck — 11.380 filas en 2,6 s],
)

== Paso 4: Modelos dbt

El proyecto dbt en `workspaces/dbt_proyect/` transforma los datos crudos en tres capas: *staging* (tipado y limpieza), *intermediate* (join entre fuentes) y *marts* (tabla analítica final).

#figure(
  image("assets/dag_dbt_obt_table.png", width: 100%),
  caption: [Lineage graph de dbt — de las fuentes raw hasta `obt_accidentes`],
)

El grafo muestra el flujo completo de dependencias: `datatran.accidentes_raw` y `datatran.clima_raw` (fuentes en MotherDuck, verde) alimentan respectivamente a `stg_accidentes` y `stg_clima` (vistas de staging, celeste). Ambos modelos convergen en `int_accidentes_clima` (vista intermedia) que resuelve el join por coordenadas y tiempo, y cuyo resultado se materializa como tabla en `obt_accidentes` (marts, púrpura).

=== Configuración

```yaml
# profiles.yml
dbt_proyect:
  outputs:
    dev:
      type: duckdb
      path: "md:airbyte_trabajo"
      schema: datatran
      motherduck_token: "{{ env_var('MOTHERDUCK_TOKEN') }}"
  target: dev
```

```yaml
# dbt_project.yml
models:
  dbt_proyect:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: view
      +schema: intermediate
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
    `REPLACE(latitude, ',', '.')::DOUBLE` y `longitude`;\
    `REPLACE(km, ',', '.')::DECIMAL`;\
    `CAST(id AS BIGINT)`, `CAST(mortos AS INT)` y demás enteros;\
    `CAST(data_inversa AS DATE)`, `CAST(horario AS TIME)`;\
    Columnas de join: `lat_r` y `lon_r` (`ROUND(latitude, 2)`)
  ],
  [`stg_clima`],
  [
    Tipado directo (ERA5 ya entrega datos numéricos);\
    `CAST(timestamp AS TIMESTAMP)`;\
    Decodificación de `weather_code` WMO a descripción legible;\
    Columnas de join: `lat_r`, `lon_r`, `fecha`, `hora` extraídos del timestamp
  ],
)

#figure(
  image("assets/dbt_staging_tables.svg", width: 100%),
  caption: [Modelos staging: `stg_accidentes` (izquierda) y `stg_clima` (derecha)],
)

==== Campos de stg_accidentes

#table(
  columns: (auto, auto, 1fr),
  table.header([*Campo*], [*Tipo*], [*Descripción*]),
  [`id_accidente`],            [`bigint`],    [Surrogate key generado por dbt (row_number) — clave primaria],
  [`id_datatran`],             [`varchar`],   [ID original de DATATRAN — informativo, sin garantía de unicidad],
  [`data_inversa`],           [`date`],      [Fecha del accidente],
  [`dia_semana`],             [`varchar`],   [Día de la semana],
  [`horario`],                [`time`],      [Hora del accidente (HH:MM:SS)],
  [`uf`],                     [`varchar`],   [Estado federativo (UF)],
  [`br`],                     [`varchar`],   [Número de ruta federal],
  [`km`],                     [`decimal`],   [Punto kilométrico],
  [`municipio`],              [`varchar`],   [Municipio del accidente],
  [`causa_acidente`],         [`varchar`],   [Causa principal],
  [`tipo_acidente`],          [`varchar`],   [Tipo de accidente],
  [`classificacao_acidente`], [`varchar`],   [Gravedad (`NULL` si era `NA`)],
  [`fase_dia`],               [`varchar`],   [Pleno dia / Anoitecer / Plena Noite / Amanhecer],
  [`sentido_via`],            [`varchar`],   [Sentido de circulación],
  [`condicao_metereologica`], [`varchar`],   [Condición climática según el agente PRF],
  [`tipo_pista`],             [`varchar`],   [Tipo de pista],
  [`tracado_via`],            [`varchar`],   [Trazado de la vía],
  [`uso_solo`],               [`varchar`],   [Urbano / Rural],
  [`pessoas`],                [`int`],       [Total de personas involucradas],
  [`mortos`],                 [`int`],       [Fallecidos],
  [`feridos_leves`],          [`int`],       [Heridos leves],
  [`feridos_graves`],         [`int`],       [Heridos graves],
  [`ilesos`],                 [`int`],       [Ilesos],
  [`ignorados`],              [`int`],       [Estado desconocido],
  [`feridos`],                [`int`],       [Total de heridos],
  [`veiculos`],               [`int`],       [Vehículos involucrados],
  [`latitude`],               [`double`],    [Latitud GPS],
  [`longitude`],              [`double`],    [Longitud GPS],
  [`lat_r`],                  [`double`],    [`ROUND(latitude, 2)` — clave de join con clima],
  [`lon_r`],                  [`double`],    [`ROUND(longitude, 2)` — clave de join con clima],
  [`regional`],               [`varchar`],   [Regional de la PRF],
  [`delegacia`],              [`varchar`],   [Delegacia de la PRF],
  [`uop`],                    [`varchar`],   [Unidad operacional de la PRF],
)

==== Campos de stg_clima

#table(
  columns: (auto, auto, 1fr),
  table.header([*Campo*], [*Tipo*], [*Descripción*]),
  [`lat_r`],                  [`double`],    [`ROUND(latitude, 2)` — clave de join],
  [`lon_r`],                  [`double`],    [`ROUND(longitude, 2)` — clave de join],
  [`timestamp_utc`],          [`timestamp`], [Timestamp horario ERA5],
  [`fecha`],                  [`date`],      [Fecha extraída del timestamp],
  [`hora`],                   [`int`],       [Hora del día (0–23) — clave de join],
  [`precipitation`],          [`double`],    [Precipitación horaria (mm/h)],
  [`weather_code`],           [`int`],       [Código WMO estandarizado],
  [`weather_desc`],           [`varchar`],   [Descripción del código WMO],
  [`temperature_2m`],         [`double`],    [Temperatura a 2 m (°C)],
  [`relative_humidity_2m`],   [`double`],    [Humedad relativa (%)],
  [`dew_point_2m`],           [`double`],    [Punto de rocío a 2 m (°C)],
  [`cloud_cover_low`],        [`int`],       [Nubes bajas (%) — proxy de niebla],
  [`wind_speed_10m`],         [`double`],    [Velocidad del viento a 10 m (km/h)],
  [`wind_gusts_10m`],         [`double`],    [Ráfagas de viento a 10 m (km/h)],
  [`shortwave_radiation`],    [`double`],    [Radiación solar (W/m²) — encandilamiento],
  [`is_day`],                 [`boolean`],   [True si es de día según ERA5],
)

=== Modelo intermedio

```
int_accidentes_clima.sql
```

Join entre `stg_accidentes` y `stg_clima` por `(lat_r, lon_r, data_inversa, HOUR(horario))`. Resultado: una fila por accidente con las variables ERA5 correspondientes al momento y lugar del siniestro.

=== Modelo OBT

Se implementa un *One Big Table* (OBT) en lugar de esquema estrella. La justificación es:

- El dataset tiene ~11.380 filas — volumen donde la normalización Kimball agrega complejidad sin beneficio de rendimiento en DuckDB/MotherDuck.
- El análisis es exploratorio: Metabase necesita filtrar y agrupar por múltiples dimensiones simultáneamente, lo que resulta más directo con una tabla ancha que con joins en tiempo de consulta.
- La integración de ERA5 ya introduce complejidad suficiente en la capa intermedia.

#table(
  columns: (auto, 1fr, auto),
  table.header([*Grupo*], [*Columnas*], [*Origen*]),
  [Identificación],  [`id_accidente`, `id_datatran`, `data_inversa`, `horario`, `dia_semana`, `fase_dia`], [DATATRAN],
  [Localización],    [`uf`, `municipio`, `br`, `km`, `latitude`, `longitude`],              [DATATRAN],
  [Causa y tipo],    [`causa_acidente`, `tipo_acidente`, `classificacao_acidente`],          [DATATRAN],
  [Vía],             [`sentido_via`, `tipo_pista`, `tracado_via`, `uso_solo`],              [DATATRAN],
  [Víctimas],        [`mortos`, `feridos_leves`, `feridos_graves`, `ilesos`, `veiculos`],   [DATATRAN],
  [Clima PRF],       [`condicao_metereologica`],                                             [DATATRAN],
  [Clima ERA5],      [`precipitation`, `weather_code`, `weather_desc`, `temperature_2m`,
                      `relative_humidity_2m`, `dew_point_2m`, `cloud_cover_low`,
                      `wind_speed_10m`, `wind_gusts_10m`, `shortwave_radiation`, `is_day`], [Open-Meteo],
)

#figure(
  image("assets/dbt_obt_table.svg", height: 25cm),
  caption: [Estructura de `obt_accidentes` — One Big Table (marts)],
)

==== Campos de obt_accidentes

#table(
  columns: (auto, auto, 1fr),
  table.header([*Campo*], [*Tipo*], [*Descripción*]),
  table.cell(colspan: 3)[*Identificación*],
  [`id_accidente`],                [`bigint`],   [Surrogate key generado por dbt — clave primaria],
  [`id_datatran`],                 [`varchar`],  [ID original de DATATRAN — sin restricción de unicidad],
  [`data_inversa`],                [`date`],     [Fecha del accidente],
  [`dia_semana`],                  [`varchar`],  [Día de la semana],
  [`horario`],                     [`time`],     [Hora del accidente],
  [`fase_dia`],                    [`varchar`],  [Pleno dia / Anoitecer / Plena Noite / Amanhecer],
  table.cell(colspan: 3)[*Localización*],
  [`uf`],                          [`varchar`],  [Estado federativo],
  [`municipio`],                   [`varchar`],  [Municipio],
  [`br`],                          [`varchar`],  [Número de ruta federal],
  [`km`],                          [`decimal`],  [Punto kilométrico],
  [`latitude`],                    [`double`],   [Latitud GPS],
  [`longitude`],                   [`double`],   [Longitud GPS],
  table.cell(colspan: 3)[*Causa y tipo*],
  [`causa_acidente`],              [`varchar`],  [Causa principal del accidente],
  [`tipo_acidente`],               [`varchar`],  [Tipo de accidente],
  [`classificacao_acidente`],      [`varchar`],  [Gravedad del accidente],
  table.cell(colspan: 3)[*Características de la vía*],
  [`sentido_via`],                 [`varchar`],  [Sentido de circulación],
  [`tipo_pista`],                  [`varchar`],  [Tipo de pista],
  [`tracado_via`],                 [`varchar`],  [Trazado de la vía],
  [`uso_solo`],                    [`varchar`],  [Uso del suelo (urbano / rural)],
  table.cell(colspan: 3)[*Víctimas y vehículos*],
  [`pessoas`],                     [`int`],      [Total de personas involucradas],
  [`mortos`],                      [`int`],      [Fallecidos],
  [`feridos_leves`],               [`int`],      [Heridos leves],
  [`feridos_graves`],              [`int`],      [Heridos graves],
  [`ilesos`],                      [`int`],      [Ilesos],
  [`ignorados`],                   [`int`],      [Estado desconocido],
  [`feridos`],                     [`int`],      [Total de heridos],
  [`veiculos`],                    [`int`],      [Vehículos involucrados],
  table.cell(colspan: 3)[*Clima registrado por PRF*],
  [`condicao_metereologica`],      [`varchar`],  [Condición subjetiva según el agente PRF],
  table.cell(colspan: 3)[*Clima ERA5 (Open-Meteo)*],
  [`clima_precipitation`],         [`double`],   [Precipitación horaria (mm/h)],
  [`clima_weather_code`],          [`int`],      [Código WMO estandarizado],
  [`clima_weather_desc`],          [`varchar`],  [Descripción del código WMO],
  [`clima_temperature_2m`],        [`double`],   [Temperatura a 2 m (°C)],
  [`clima_relative_humidity_2m`],  [`double`],   [Humedad relativa (%)],
  [`clima_dew_point_2m`],          [`double`],   [Punto de rocío (°C) — riesgo de escarcha],
  [`clima_cloud_cover_low`],       [`int`],      [Nubes bajas (%) — proxy de niebla],
  [`clima_wind_speed_10m`],        [`double`],   [Velocidad del viento (km/h)],
  [`clima_wind_gusts_10m`],        [`double`],   [Ráfagas de viento (km/h)],
  [`clima_shortwave_radiation`],   [`double`],   [Radiación solar (W/m²) — encandilamiento],
  [`clima_is_day`],                [`boolean`],  [True si es de día según ERA5],
  [`clima_join_match`],            [`boolean`],  [True si se encontró dato ERA5 para el accidente],
)

=== Tests de calidad (dbt-expectations)

#table(
  columns: (2fr, 2fr, 1fr),
  table.header([*Test*], [*Modelo*], [*Dimensión de calidad*]),
  [#dbt("unique") + #dbt("not_null")],                       [#dbt("obt_accidentes.id_accidente")], [Unicidad / completitud],
  [#dbt("expect_column_values_to_be_between")],              [#dbt("stg_accidentes.mortos")],      [Validez: rango 0–100],
  [#dbt("expect_column_values_to_be_between")],              [#dbt("stg_accidentes.latitude")],    [Validez: rango -34 a 6],
  [#dbt("expect_column_values_to_not_be_null")],             [#dbt("stg_accidentes.uf")],          [Completitud],
  [#dbt("expect_column_proportion_of_unique_values")],       [#dbt("stg_accidentes.municipio")],   [Consistencia: >100 municipios],
  [#dbt("expect_column_values_to_match_regex")],             [#dbt("stg_accidentes.data_inversa")],[Formato YYYY-MM-DD],
)

== Paso 5: Orquestación con Prefect

El pipeline se orquesta con Prefect 3 desde `workspaces/prefect/pipeline.py`. Cubre las 7 etapas del pipeline de extremo a extremo, incluyendo la gestión de la infraestructura Docker y la verificación de datos antes de cada transformación.

=== Estructura del pipeline

```python
@flow(name="datatran_pipeline", log_prints=True)
def datatran_pipeline():
    ensure_containers_up()    # docker compose up -d + espera MySQL
    load_accidentes_raw()     # convierte CSVs, recrea tabla, LOAD DATA LOCAL INFILE
    extract_openmeteo()       # extrae ERA5 y genera data/clima_openmeteo.csv
    load_clima_raw()          # crea tabla + LOAD DATA LOCAL INFILE en MySQL
    airbyte_sync()            # dispara sync y polling vía API REST
    dbt_run()                 # dbt deps + dbt run
    dbt_test()                # dbt test
```

#table(
  columns: (auto, auto, 1fr),
  table.header([*Task*], [*Nombre*], [*Descripción*]),
  [1], [#dbt("ensure_containers_up")],  [Ejecuta `docker compose up -d` y espera hasta que MySQL acepte conexiones (máx 60 s). Reintentos: 2.],
  [2], [#dbt("load_accidentes_raw")],  [Ejecuta `convert_csv_to_utf8.py` para convertir CSVs nuevos. Luego recrea `accidentes_raw` (DROP + CREATE vía `01_schema.sql`) y carga todos los `*_utf8.csv` de `data/` con LOAD DATA LOCAL INFILE. Reintentos: 1.],
  [3], [#dbt("extract_openmeteo")],     [Lee coordenadas únicas por fecha desde MySQL, las consulta en lotes de 100 a la API ERA5 de Open-Meteo y escribe `data/clima_openmeteo.csv`. Soporta reanudación. Timeout: 30 min.],
  [4], [#dbt("load_clima_raw")],        [Crea la tabla `clima_raw` en MySQL (si no existe) y ejecuta `LOAD DATA INFILE`. Saltea la carga si la tabla ya tiene filas.],
  [5], [#dbt("airbyte_sync")],          [Dispara el sync `MySQL_Datatran → MotherDuck_datatran` vía API REST y hace polling cada 10 s hasta completar o fallar. Timeout: 10 min.],
  [6], [#dbt("dbt_run")],               [Ejecuta `dbt deps` + `dbt run`. Reintentos: 1.],
  [7], [#dbt("dbt_test")],              [Ejecuta `dbt test` y falla el flow si algún test no pasa.],
)

=== Configuración

```bash
cd workspaces/prefect
cp example.env .env     # completar MYSQL_PASSWORD y AIRBYTE_CONNECTION_ID
pip install prefect mysql-connector-python
prefect server start &
python pipeline.py
```

La variable `AIRBYTE_CONNECTION_ID` se obtiene en Airbyte UI → Connections → Settings → Connection ID.

=== Resultado de la ejecución

#figure(
  image("assets/prefect_graphs.png", width: 100%),
  caption: [Dashboard de Prefect — historial de ejecuciones del flow `datatran_pipeline`],
)

El dashboard de Prefect registra 9 ejecuciones del flow `datatran_pipeline` durante el desarrollo del pipeline. Las 8 ejecuciones fallidas (rojo) corresponden a las iteraciones de corrección —errores de autenticación en Airbyte, ajuste de rutas dbt, resolución de duplicados en `stg_clima`— mientras que la última ejecución (verde) completó exitosamente las 7 tasks con 28 task runs completados sobre 36 totales. Los 8 task runs fallidos pertenecen a ejecuciones anteriores acumuladas en el historial.

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
  [Nombre para mostrar],   [`Motherduck_Trabajo_Final`],
  [Database file],         [`md:airbyte_trabajo`],
  [MotherDuck Token],      [Token de la cuenta (campo separado del driver)],
  [Use DuckDB old\_implicit\_casting option], [Activado],
)

#figure(
  image("assets/configure_motherduck_metabase.png", width: 72%),
  caption: [Configuración de la conexión DuckDB/MotherDuck en Metabase],
)

=== Dashboard implementado

Las visualizaciones se organizaron en un único dashboard de Metabase con cinco paneles, todos alimentados por la tabla `marts.obt_accidentes` en MotherDuck mediante preguntas SQL con variables de filtro.

==== Filtros interactivos

El dashboard expone dos filtros que se propagan simultáneamente a todos los paneles:

- *Estado* — selector de UF (campo `uf`); permite aislar cualquier estado federativo.
- *Fechas* — rango de fechas sobre `data_inversa` (tipo DATE); soporta períodos relativos como "Previous 12 months" o rangos manuales.

==== Paneles del dashboard

El dashboard contiene cinco visualizaciones dispuestas en grilla:

#table(
  columns: (auto, auto, 1fr),
  table.header([*Panel*], [*Tipo*], [*Pregunta de negocio*]),
  [1], [Dona],               [¿Cuáles son las 5 principales causas de accidentes y su participación?],
  [2], [Barras],             [¿Cómo se distribuyen los accidentes por día de la semana?],
  [3], [Líneas doble eje],   [¿Cómo evolucionan accidentes y muertes a lo largo de la semana?],
  [4], [Dona],               [¿Qué condición climática concentra más accidentes?],
  [5], [Barras agrupadas],   [¿Qué condición climática genera más muertes y heridos?],
)

==== Vista sin filtros — dataset completo (11.380 accidentes)

#figure(
  image("assets/metabase_visualization_unfiltered.png", width: 100%),
  caption: [Dashboard completo — 11.380 accidentes, todos los estados y período completo],
)

Con el dataset completo se observa que *Céu Claro* concentra el 55.42% de los accidentes y domina visiblemente el panel de condiciones climáticas. Las causas principales —*Ausência de reação* (30.2%) y *Reação tardia* (28.8%)— acumulan casi el 60% de los casos, confirmando que la atención del conductor es el factor determinante por encima del clima o la infraestructura. En cuanto a la distribución semanal, los accidentes crecen hacia el fin de semana y la curva de muertes sigue esa tendencia con mayor pendiente, indicando mayor severidad relativa en sábados y domingos.

==== Vista filtrada — Estado RJ, período reciente (525 accidentes)

#figure(
  image("assets/metabase_visualization_filtered.png", width: 100%),
  caption: [Dashboard filtrado — Estado RJ, Previous 12 months — 525 accidentes],
)

Al filtrar por el estado de Río de Janeiro se reduce el conjunto a 525 accidentes. La distribución de causas se mantiene estable respecto al total nacional (*Ausência de reação* sigue siendo la primera causa con ~34%), lo que sugiere que el patrón de comportamiento del conductor es consistente entre estados. En el panel de condiciones climáticas, *Céu Claro* continúa dominando (48.76%), seguido de *Nublado* (20.85%) y *Chuva* (19.56%) —una proporción de lluvia notablemente mayor que en el promedio nacional, consistente con el clima de RJ. El panel de condiciones por gravedad muestra que *Céu Claro* y *Chuva* lideran en heridos leves y graves, mientras que *Vento* y *Nevoeiro/Neblina* presentan mayores muertes relativas pese a su bajo volumen.

== Conclusiones

El pipeline ELT implementado integra dos fuentes de datos heterogéneas —accidentes de tráfico en formato CSV con problemas de encoding y campos compuestos, y datos climáticos históricos de una API REST gratuita— en un modelo dimensional analítico en MotherDuck.

Las decisiones técnicas más relevantes fueron:

- *Encoding y pre-procesamiento fuera del contenedor:* la conversión Latin-1 → UTF-8 se realiza con un script Python antes de levantar MySQL, manteniendo la ingesta SQL simple y sin dependencias externas en el contenedor.

- *Todo TEXT en la capa raw:* evita que MySQL rechace o trunque datos con formatos no estándar (decimales con coma, nulos literales `NA`, fechas como string). dbt es el único responsable del tipado, lo que hace la transformación auditable y testeable.

- *Open-Meteo ERA5 sobre OpenWeather:* la API gratuita provee más variables relevantes para el análisis vial (ráfagas, nubes bajas, radiación solar) y no requiere gestión de claves API, simplificando la configuración y el despliegue.

- *One Big Table (OBT):* con ~11.380 filas, la normalización Kimball agrega complejidad sin beneficio de rendimiento en DuckDB/MotherDuck. El OBT simplifica las consultas de Metabase —todos los filtros y agrupaciones sobre una sola tabla— y es coherente con la ya compleja capa intermedia que resuelve el join con ERA5.

#bibliography("refs.bib", title: "Referencias", style: "ieee")
