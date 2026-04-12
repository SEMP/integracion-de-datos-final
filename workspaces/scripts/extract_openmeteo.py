#!/usr/bin/env python3
"""
extract_openmeteo.py — Extrae datos climáticos ERA5 de Open-Meteo
leyendo las coordenadas únicas desde MySQL (datatran.accidentes_raw).

Estrategia:
  · Por cada fecha con accidentes, obtiene las coordenadas únicas
    redondeadas a 2 decimales (~1 km) y las consulta en lotes de
    BATCH_SIZE a la API ERA5 de Open-Meteo.
  · Escribe el resultado en data/clima_openmeteo.csv (separador ;).
  · Reanudación segura: una fecha se registra como completada en
    data/clima_openmeteo.progress SOLO después de que TODOS sus batches
    terminaron exitosamente. Si el script se interrumpe a mitad de una
    fecha, esa fecha se reprocesa completa en la siguiente ejecución
    (los batches ya escritos quedan en el CSV; los datos duplicados se
    eliminan al cargar en MySQL con DROP TABLE IF EXISTS).

Dependencias:
  · mysql-connector-python  (pip install mysql-connector-python)
  · stdlib: csv, json, time, urllib, collections, pathlib
"""

import csv
import json
import sys
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
SCRIPT_DIR    = Path(__file__).resolve().parent
DATA_DIR      = (SCRIPT_DIR / ".." / ".." / "data").resolve()
ENV_FILE      = SCRIPT_DIR / ".env"
OUTPUT_CSV    = DATA_DIR / "clima_openmeteo.csv"
PROGRESS_FILE = DATA_DIR / "clima_openmeteo.progress"  # fechas 100% completadas

# ---------------------------------------------------------------------------
# Configuración API
# ---------------------------------------------------------------------------
BATCH_SIZE       = 100
DELAY_SEC        = 2      # pausa base entre requests
DELAY_429_SEC    = 60     # espera fija ante 429 (no crece — máx 60s por reintento)
MAX_RETRIES_429  = 4      # después de 4 reintentos, abandona el batch y continúa
TIMEZONE         = "America%2FSao_Paulo"

HOURLY_VARS = [
    "precipitation",
    "weather_code",
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "cloud_cover_low",
    "wind_speed_10m",
    "wind_gusts_10m",
    "shortwave_radiation",
    "is_day",
]
FIELDNAMES = ["latitude", "longitude", "timestamp"] + HOURLY_VARS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_env(path: Path) -> dict:
    env = {}
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def load_completed_dates() -> set:
    """Lee el archivo .progress y devuelve el conjunto de fechas ya completadas."""
    if not PROGRESS_FILE.exists():
        return set()
    return set(PROGRESS_FILE.read_text(encoding="utf-8").splitlines())


def mark_date_complete(fecha: str) -> None:
    """Agrega una fecha al archivo .progress (append, una línea por fecha)."""
    with open(PROGRESS_FILE, "a", encoding="utf-8") as f:
        f.write(fecha + "\n")


def fetch_coords_by_date(env: dict) -> dict:
    """
    Conecta a MySQL y devuelve {fecha_str: [(lat, lon), ...]}
    con coordenadas únicas redondeadas a 2 decimales por fecha.
    """
    import mysql.connector

    con = mysql.connector.connect(
        host     = env.get("MYSQL_HOST", "127.0.0.1"),
        port     = int(env.get("MYSQL_PORT", 3306)),
        user     = env.get("MYSQL_USER", "airbyte"),
        password = env.get("MYSQL_PASSWORD", ""),
        database = env.get("MYSQL_DATABASE", "datatran"),
    )
    cursor = con.cursor()
    cursor.execute("""
        SELECT
            data_inversa                                                   AS fecha,
            ROUND(CAST(REPLACE(latitude,  ',', '.') AS DECIMAL(10,6)), 2) AS lat,
            ROUND(CAST(REPLACE(longitude, ',', '.') AS DECIMAL(10,6)), 2) AS lon
        FROM accidentes_raw
        WHERE latitude  NOT IN ('NA', '')
          AND longitude NOT IN ('NA', '')
        GROUP BY fecha, lat, lon
        ORDER BY fecha, lat, lon
    """)
    rows = cursor.fetchall()
    cursor.close()
    con.close()

    by_date = defaultdict(list)
    for fecha, lat, lon in rows:
        by_date[str(fecha)].append((float(lat), float(lon)))
    return dict(sorted(by_date.items()))


def call_openmeteo(lats: list, lons: list, date: str) -> list:
    """
    Llama a la API ERA5 de Open-Meteo para un lote de coordenadas en una fecha.
    Reintenta ante errores 429 con backoff exponencial.
    Devuelve lista de dicts listos para escribir al CSV.
    """
    import urllib.error

    lat_str = ",".join(str(x) for x in lats)
    lon_str = ",".join(str(x) for x in lons)
    hourly  = ",".join(HOURLY_VARS)

    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat_str}"
        f"&longitude={lon_str}"
        f"&start_date={date}"
        f"&end_date={date}"
        f"&hourly={hourly}"
        f"&timezone={TIMEZONE}"
    )

    for attempt in range(1, MAX_RETRIES_429 + 2):  # intento 1 + hasta 4 reintentos
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                data = json.loads(resp.read())
            break  # éxito
        except urllib.error.HTTPError as e:
            if e.code == 429:
                if attempt > MAX_RETRIES_429:
                    raise  # agotó reintentos → el caller lo captura como WARN
                print(f"    429 — esperando {DELAY_429_SEC}s (intento {attempt}/{MAX_RETRIES_429})...",
                      flush=True)
                time.sleep(DELAY_429_SEC)  # espera fija, sin crecer
            else:
                raise

    if isinstance(data, dict):
        data = [data]

    rows = []
    for loc in data:
        lat = round(float(loc["latitude"]),  2)
        lon = round(float(loc["longitude"]), 2)
        hourly_data = loc.get("hourly", {})
        times = hourly_data.get("time", [])
        for i, ts in enumerate(times):
            row = {"latitude": lat, "longitude": lon, "timestamp": ts}
            for var in HOURLY_VARS:
                vals = hourly_data.get(var, [])
                v = vals[i] if i < len(vals) else None
                row[var] = "" if v is None else v
            rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    env = load_env(ENV_FILE)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("Consultando coordenadas únicas por fecha desde MySQL...")
    by_date = fetch_coords_by_date(env)
    total_dates = len(by_date)
    total_pairs = sum(len(v) for v in by_date.values())
    print(f"  {total_dates} fechas | {total_pairs} pares únicos (fecha, coord)")

    # Fechas 100% completadas según el archivo .progress
    completed = load_completed_dates()
    pending   = total_dates - len(completed)
    if completed:
        print(f"  Retomando — {len(completed)} fechas completadas, {pending} pendientes")

    # El CSV se abre en append si ya existe (puede tener filas de una ejecución
    # anterior interrumpida; se reescriben al recargar en MySQL con DROP TABLE)
    write_header = not OUTPUT_CSV.exists()
    with open(OUTPUT_CSV, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=FIELDNAMES, delimiter=";",
            quoting=csv.QUOTE_MINIMAL, lineterminator="\n",
        )
        if write_header:
            writer.writeheader()

        for d_idx, (fecha, coords) in enumerate(by_date.items(), 1):
            if fecha in completed:
                continue

            batches = [
                coords[i : i + BATCH_SIZE]
                for i in range(0, len(coords), BATCH_SIZE)
            ]
            print(
                f"  [{d_idx:3d}/{total_dates}] {fecha} — "
                f"{len(coords):3d} coords | {len(batches)} requests",
                flush=True,
            )

            batch_errors = 0
            for batch in batches:
                lats = [c[0] for c in batch]
                lons = [c[1] for c in batch]
                try:
                    rows = call_openmeteo(lats, lons, fecha)
                    writer.writerows(rows)
                    f.flush()  # asegurar escritura a disco antes de continuar
                except Exception as exc:
                    print(f"    WARN: error en batch {fecha} — {exc}")
                    batch_errors += 1
                time.sleep(DELAY_SEC)

            if batch_errors == 0:
                # Solo marcar como completa si TODOS los batches tuvieron éxito
                mark_date_complete(fecha)
            else:
                print(f"    WARN: {fecha} quedó incompleta ({batch_errors} batches fallidos)")

    completed_now = load_completed_dates()
    pending = total_dates - len(completed_now)
    print(f"\nListo — {len(completed_now)}/{total_dates} fechas completadas")
    print(f"  CSV:      {OUTPUT_CSV}")
    print(f"  Progress: {PROGRESS_FILE}")

    if pending > 0:
        print(f"\nATENCION: {pending} fechas pendientes. Volver a ejecutar para completar.")
        sys.exit(1)   # señaliza al task de Prefect que la extracción está incompleta


if __name__ == "__main__":
    main()
