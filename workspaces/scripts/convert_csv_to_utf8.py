"""
convert_csv_to_utf8.py
======================
Lee todos los archivos .csv encontrados en CSV_SOURCE_DIR (definido en .env),
los convierte de Latin-1 / CRLF a UTF-8 / LF y los escribe en ../../data/
(carpeta data/ en la raíz del proyecto).

Rutas clave (relativas a este script):
    .env de configuración : workspaces/scripts/.env
    Carpeta de salida     : ../../data/  →  <raíz del proyecto>/data/

Configuración:
    cp workspaces/scripts/example.env workspaces/scripts/.env
    # Editar .env y ajustar CSV_SOURCE_DIR

Uso:
    # Procesa todos los .csv de CSV_SOURCE_DIR → data/
    python3 workspaces/scripts/convert_csv_to_utf8.py

    # Un archivo específico (sin leer .env):
    python3 workspaces/scripts/convert_csv_to_utf8.py \\
        --input  /ruta/datatran2026.csv \\
        --output /ruta/de/salida/datatran2026_utf8.csv
"""

import csv
import argparse
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR   = (SCRIPT_DIR / ".." / ".." / "data").resolve()
ENV_FILE   = SCRIPT_DIR / ".env"


def load_env(path: Path) -> dict:
    """Parser mínimo de archivos .env (sin dependencias externas).
    Soporta comentarios (#), líneas en blanco y valores con/sin comillas.
    """
    env = {}
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def convert(input_path: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"  Leyendo    : {input_path}")
    print(f"  Escribiendo: {output_path}")

    rows_written = 0
    with (
        open(input_path, encoding="latin-1", newline="") as src,
        open(output_path, "w", encoding="utf-8", newline="\n") as dst,
    ):
        reader = csv.reader(src, delimiter=";", quotechar='"')
        writer = csv.writer(
            dst,
            delimiter=";",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )
        for row in reader:
            # Limpiar espacios sobrantes; preservar 'NA' (dbt los convierte con NULLIF)
            writer.writerow([field.strip() for field in row])
            rows_written += 1

    print(f"  Filas escritas (incluye encabezado): {rows_written}")


def main():
    parser = argparse.ArgumentParser(
        description="Convierte CSV(s) Latin-1/CRLF → UTF-8/LF y los guarda en data/"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Ruta a un CSV específico (omitir para procesar todos los de CSV_SOURCE_DIR)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Ruta de salida (solo válido junto con --input)",
    )
    args = parser.parse_args()

    # --- Modo manual: un archivo concreto pasado por argumento ---
    if args.input:
        output = args.output or DATA_DIR / (args.input.stem + "_utf8.csv")
        convert(args.input, output)
        print("Listo.")
        return

    # --- Modo automático: leer CSV_SOURCE_DIR desde .env ---
    env = load_env(ENV_FILE)
    source_dir_str = env.get("CSV_SOURCE_DIR", "").strip()

    if not source_dir_str:
        print(
            f"Error: CSV_SOURCE_DIR no está definido.\n"
            f"  1. Copiá '{ENV_FILE.parent / 'example.env'}' a '{ENV_FILE}'\n"
            f"  2. Ajustá CSV_SOURCE_DIR con la ruta al directorio de los CSV originales."
        )
        raise SystemExit(1)

    source_dir = Path(source_dir_str).expanduser().resolve()
    if not source_dir.is_dir():
        print(f"Error: CSV_SOURCE_DIR='{source_dir}' no es un directorio válido.")
        raise SystemExit(1)

    csv_files = sorted(
        f for f in source_dir.glob("*.csv")
        if not f.stem.endswith("_utf8")
    )
    if not csv_files:
        print(f"No se encontraron archivos .csv (sin sufijo _utf8) en: {source_dir}")
        raise SystemExit(1)

    print(f"Origen  : {source_dir}")
    print(f"Destino : {DATA_DIR}")
    print(f"Archivos: {len(csv_files)}\n")

    for csv_file in csv_files:
        output = DATA_DIR / (csv_file.stem + "_utf8.csv")
        convert(csv_file, output)
        print()

    print("Conversión completada.")


if __name__ == "__main__":
    main()
