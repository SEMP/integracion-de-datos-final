#!/usr/bin/env python3
"""
pipeline.py — Orquestación del pipeline ELT con Prefect.

Secuencia:
  1. ensure_containers_up    — docker compose up -d + espera MySQL ready
  2. verify_accidentes_raw   — verifica datos en MySQL; si faltan, corre initdb
  3. extract_openmeteo       — extrae clima ERA5 y genera data/clima_openmeteo.csv
  4. load_clima_raw          — crea tabla clima_raw y carga el CSV en MySQL
  5. airbyte_sync            — dispara el sync y hace polling hasta completar
  6. dbt_run                 — dbt deps + dbt run
  7. dbt_test                — dbt test

Configuración: leer workspaces/prefect/.env (copiar de example.env)

Ejecutar:
  cd workspaces/prefect
  prefect server start &          # si no está corriendo
  python pipeline.py
"""

import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
import json
from pathlib import Path

from prefect import flow, task, get_run_logger

# ---------------------------------------------------------------------------
# Rutas base
# ---------------------------------------------------------------------------
PIPELINE_DIR    = Path(__file__).resolve().parent
WORKSPACE_DIR   = PIPELINE_DIR.parent
PROJECT_DIR     = WORKSPACE_DIR.parent
CONTAINERS_DIR  = WORKSPACE_DIR / "containers"
SCRIPTS_DIR     = WORKSPACE_DIR / "scripts"
DBT_DIR         = WORKSPACE_DIR / "dbt_proyecto"
DATA_DIR        = PROJECT_DIR / "data"
INITDB_DIR      = CONTAINERS_DIR / "initdb"

# ---------------------------------------------------------------------------
# Configuración (.env)
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

ENV = load_env(PIPELINE_DIR / ".env")

MYSQL_HOST      = ENV.get("MYSQL_HOST",      "127.0.0.1")
MYSQL_PORT      = int(ENV.get("MYSQL_PORT",  "3306"))
MYSQL_USER      = ENV.get("MYSQL_USER",      "airbyte")
MYSQL_PASSWORD  = ENV.get("MYSQL_PASSWORD",  "")
MYSQL_DATABASE  = ENV.get("MYSQL_DATABASE",  "datatran")
AIRBYTE_URL     = ENV.get("AIRBYTE_URL",     "http://localhost:8000")
AIRBYTE_CONN_ID = ENV.get("AIRBYTE_CONNECTION_ID", "")
MYSQL_CONTAINER = ENV.get("MYSQL_CONTAINER", "tf-mysql")

# ---------------------------------------------------------------------------
# Task 1 — Contenedores Docker
# ---------------------------------------------------------------------------

@task(name="ensure_containers_up", retries=2, retry_delay_seconds=10)
def ensure_containers_up():
    logger = get_run_logger()
    logger.info("Levantando contenedores Docker...")

    result = subprocess.run(
        ["docker", "compose", "up", "-d"],
        cwd=CONTAINERS_DIR,
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"docker compose up falló:\n{result.stderr}")
    logger.info("Contenedores iniciados. Esperando MySQL...")

    # Esperar hasta que MySQL acepte conexiones (máx 60 s)
    import mysql.connector
    for attempt in range(12):
        try:
            con = mysql.connector.connect(
                host=MYSQL_HOST, port=MYSQL_PORT,
                user=MYSQL_USER, password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE, connection_timeout=5,
            )
            con.close()
            logger.info("MySQL listo.")
            return
        except Exception:
            time.sleep(5)

    raise RuntimeError("MySQL no respondió en 60 segundos.")


# ---------------------------------------------------------------------------
# Task 2 — Verificar datos en accidentes_raw
# ---------------------------------------------------------------------------

@task(name="verify_accidentes_raw", retries=1)
def verify_accidentes_raw():
    logger = get_run_logger()
    import mysql.connector

    con = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )
    cursor = con.cursor()

    # Verificar que la tabla existe
    cursor.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = %s AND table_name = 'accidentes_raw'
    """, (MYSQL_DATABASE,))
    table_exists = cursor.fetchone()[0] > 0

    count = 0
    if table_exists:
        cursor.execute("SELECT COUNT(*) FROM accidentes_raw")
        count = cursor.fetchone()[0]

    cursor.close()
    con.close()

    if count > 0:
        logger.info(f"accidentes_raw verificado: {count:,} registros.")
        return count

    # Si la tabla está vacía o no existe, intentar correr los initdb scripts
    logger.warning(
        "accidentes_raw está vacío o no existe. "
        "Intentando ejecutar scripts initdb..."
    )

    csv_path = DATA_DIR / "datatran2026_utf8.csv"
    if not csv_path.exists():
        raise RuntimeError(
            f"No se encontró {csv_path}.\n"
            "Ejecutá primero: python3 workspaces/scripts/convert_csv_to_utf8.py"
        )

    for script in sorted(INITDB_DIR.glob("0[0-9]*.sql")):
        logger.info(f"  Ejecutando {script.name}...")
        with open(script, "rb") as f:
            result = subprocess.run(
                ["docker", "exec", "-i", MYSQL_CONTAINER,
                 "mysql", f"-u{MYSQL_USER}", f"-p{MYSQL_PASSWORD}", MYSQL_DATABASE],
                input=f.read(), capture_output=True,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"{script.name} falló:\n{result.stderr.decode()}"
                )

    # Verificar de nuevo
    con = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )
    cursor = con.cursor()
    cursor.execute("SELECT COUNT(*) FROM accidentes_raw")
    count = cursor.fetchone()[0]
    cursor.close()
    con.close()

    if count == 0:
        raise RuntimeError(
            "accidentes_raw sigue vacío después de correr los scripts. "
            "Verificá que CSV_DIR en containers/.env apunte a data/."
        )

    logger.info(f"accidentes_raw cargado: {count:,} registros.")
    return count


# ---------------------------------------------------------------------------
# Task 3 — Extracción Open-Meteo ERA5
# ---------------------------------------------------------------------------

@task(name="extract_openmeteo")
def extract_openmeteo():
    """
    Llama a extract_openmeteo.py, que maneja internamente la reanudación:
    - Salta fechas ya completadas según data/clima_openmeteo.progress
    - Una fecha solo se marca completa si TODOS sus batches tuvieron éxito
    - Si el CSV existe pero una fecha está incompleta, se reprocesa
    El script siempre se ejecuta; la idempotencia la maneja el script mismo.
    """
    logger = get_run_logger()
    progress = DATA_DIR / "clima_openmeteo.progress"
    output   = DATA_DIR / "clima_openmeteo.csv"

    if progress.exists():
        completed = len(progress.read_text(encoding="utf-8").splitlines())
        logger.info(f"Retomando extracción Open-Meteo ({completed} fechas ya completadas).")
    else:
        logger.info("Iniciando extracción Open-Meteo ERA5 desde cero...")

    result = subprocess.run(
        [sys.executable, str(SCRIPTS_DIR / "extract_openmeteo.py")],
        capture_output=False,   # mostrar progreso en tiempo real
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError("extract_openmeteo.py falló.")

    if not output.exists():
        raise RuntimeError(f"El CSV no fue generado en {output}.")

    logger.info(f"Extracción completada: {output}")
    return str(output)


# ---------------------------------------------------------------------------
# Task 4 — Cargar clima_raw en MySQL
# ---------------------------------------------------------------------------

@task(name="load_clima_raw", retries=1)
def load_clima_raw():
    logger = get_run_logger()
    import mysql.connector

    schema_sql = INITDB_DIR / "03_clima_schema.sql"
    csv_path   = DATA_DIR / "clima_openmeteo.csv"

    if not csv_path.exists():
        raise RuntimeError(f"No se encontró {csv_path}.")

    # Crear tabla (DROP TABLE IF EXISTS + CREATE) vía docker exec con root
    logger.info("Creando tabla clima_raw...")
    with open(schema_sql, "rb") as f:
        result = subprocess.run(
            ["docker", "exec", "-i", MYSQL_CONTAINER,
             "mysql", f"-u{MYSQL_USER}", f"-p{MYSQL_PASSWORD}", MYSQL_DATABASE],
            input=f.read(), capture_output=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"03_clima_schema.sql falló:\n{result.stderr.decode()}")

    # Verificar si ya tiene datos (por si el pipeline se reintenta)
    con = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        allow_local_infile=True,
    )
    cursor = con.cursor()
    cursor.execute("SELECT COUNT(*) FROM clima_raw")
    count = cursor.fetchone()[0]

    if count > 0:
        cursor.close()
        con.close()
        logger.info(f"clima_raw ya tiene {count:,} filas. Saltando LOAD DATA.")
        return count

    # Cargar CSV usando LOCAL INFILE (no requiere privilegio FILE en el server)
    # El server ya tiene --local-infile=1 configurado en docker-compose.yaml
    logger.info("Cargando clima_openmeteo.csv en MySQL vía LOCAL INFILE...")
    load_sql = """
        LOAD DATA LOCAL INFILE %s
        INTO TABLE clima_raw
        CHARACTER SET utf8mb4
        FIELDS TERMINATED BY ';' ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
    """
    cursor.execute(load_sql, (str(csv_path),))
    con.commit()
    count = cursor.rowcount
    cursor.close()
    con.close()

    logger.info(f"clima_raw cargado: {count:,} filas.")
    return count


# ---------------------------------------------------------------------------
# Task 5 — Airbyte sync
# ---------------------------------------------------------------------------

@task(name="airbyte_sync", timeout_seconds=600)
def airbyte_sync():
    logger = get_run_logger()

    if not AIRBYTE_CONN_ID:
        raise RuntimeError("AIRBYTE_CONNECTION_ID no configurado en .env")

    # Disparar sync
    url     = f"{AIRBYTE_URL}/api/v1/connections/sync"
    payload = json.dumps({"connectionId": AIRBYTE_CONN_ID}).encode()
    req     = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        job = json.loads(resp.read())

    job_id = job["job"]["id"]
    logger.info(f"Sync iniciado — job_id={job_id}")

    # Polling cada 10 s hasta completar
    status_url = f"{AIRBYTE_URL}/api/v1/jobs/get"
    while True:
        time.sleep(10)
        req = urllib.request.Request(
            status_url,
            data=json.dumps({"id": job_id}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            status = json.loads(resp.read())["job"]["status"]

        logger.info(f"  job {job_id} — status: {status}")

        if status == "succeeded":
            logger.info("Sync completado exitosamente.")
            return
        if status in ("failed", "cancelled", "incomplete"):
            raise RuntimeError(f"Airbyte sync terminó con status: {status}")


# ---------------------------------------------------------------------------
# Task 6 — dbt run
# ---------------------------------------------------------------------------

@task(name="dbt_run", retries=1)
def dbt_run():
    logger = get_run_logger()
    for cmd in [["dbt", "deps"], ["dbt", "run"]]:
        logger.info(f"Ejecutando: {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=DBT_DIR, capture_output=True, text=True)
        logger.info(result.stdout[-2000:])
        if result.returncode != 0:
            raise RuntimeError(f"{' '.join(cmd)} falló:\n{result.stderr}")


# ---------------------------------------------------------------------------
# Task 7 — dbt test
# ---------------------------------------------------------------------------

@task(name="dbt_test")
def dbt_test():
    logger = get_run_logger()
    logger.info("Ejecutando: dbt test")
    result = subprocess.run(
        ["dbt", "test"], cwd=DBT_DIR, capture_output=True, text=True,
    )
    logger.info(result.stdout[-2000:])
    if result.returncode != 0:
        raise RuntimeError(f"dbt test falló:\n{result.stderr}")
    logger.info("Todos los tests pasaron.")


# ---------------------------------------------------------------------------
# Flow principal
# ---------------------------------------------------------------------------

@flow(name="datatran_pipeline", log_prints=True)
def datatran_pipeline():
    ensure_containers_up()
    verify_accidentes_raw()
    extract_openmeteo()
    load_clima_raw()
    airbyte_sync()
    dbt_run()
    dbt_test()


if __name__ == "__main__":
    datatran_pipeline()
