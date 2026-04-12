#!/usr/bin/env python3
"""
extract_openmeteo.py — Extrae datos climáticos ERA5 de Open-Meteo
leyendo las coordenadas únicas desde MySQL (datatran.accidentes_raw).

Estrategia:
  · Por cada fecha con accidentes, obtiene las coordenadas únicas
    redondeadas a 2 decimales (~1 km) y las consulta en lotes de
    BATCH_SIZE a la API ERA5 de Open-Meteo.
  · Escribe el resultado en data/clima_openmeteo.csv (separador ;).
  · Soporta reanudación: si el CSV ya existe, omite las fechas procesadas.

Dependencias:
  · mysql-connector-python  (pip install mysql-connector-python)
  · stdlib: csv, json, time, urllib, collections, pathlib
"""

import csv
import json
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR   = (SCRIPT_DIR / ".." / ".." / "data").resolve()
ENV_FILE   = SCRIPT_DIR / ".env"
OUTPUT_CSV = DATA_DIR / "clima_openmeteo.csv"

# ---------------------------------------------------------------------------
# Configuración API
# ---------------------------------------------------------------------------
BATCH_SIZE  = 100
DELAY_SEC   = 0.5
TIMEZONE    = "America%2FSao_Paulo"

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
    Devuelve lista de dicts listos para escribir al CSV.
    """
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

    with urllib.request.urlopen(url, timeout=30) as resp:
        data = json.loads(resp.read())

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

    already_done: set = set()
    if OUTPUT_CSV.exists():
        with open(OUTPUT_CSV, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                already_done.add(row["timestamp"][:10])
        print(f"  Retomando — {len(already_done)} fechas ya en el CSV")

    mode = "a" if already_done else "w"
    with open(OUTPUT_CSV, mode, encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=FIELDNAMES, delimiter=";",
            quoting=csv.QUOTE_MINIMAL, lineterminator="\n",
        )
        if mode == "w":
            writer.writeheader()

        for d_idx, (fecha, coords) in enumerate(by_date.items(), 1):
            if fecha in already_done:
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

            for batch in batches:
                lats = [c[0] for c in batch]
                lons = [c[1] for c in batch]
                try:
                    rows = call_openmeteo(lats, lons, fecha)
                    writer.writerows(rows)
                except Exception as exc:
                    print(f"    WARN: error en batch {fecha} — {exc}")
                time.sleep(DELAY_SEC)

    print(f"\nListo. CSV guardado en:\n  {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
