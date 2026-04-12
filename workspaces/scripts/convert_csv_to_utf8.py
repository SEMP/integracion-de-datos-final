"""
convert_csv_to_utf8.py
======================
Convierte datatran2026.csv (Latin-1, CRLF, separador ;) a UTF-8 con LF.

El archivo original NO se modifica. Se escribe un nuevo archivo _utf8.csv
en el mismo directorio, listo para ser montado en el contenedor MySQL y
cargado con LOAD DATA INFILE.

Uso:
    python3 workspaces/scripts/convert_csv_to_utf8.py

    # O con paths explícitos:
    python3 workspaces/scripts/convert_csv_to_utf8.py \
        --input  /ruta/datatran2026.csv \
        --output /ruta/datatran2026_utf8.csv
"""

import csv
import argparse
from pathlib import Path

DEFAULT_INPUT = Path(
    "/home/sergio/Documents/Facultad/Maestria/"
    "09-MIA_3_Introduccion_a_la_integracion_de_datos/"
    "workspaces/data/datatran2026.csv"
)


def convert(input_path: Path, output_path: Path) -> None:
    print(f"Leyendo  : {input_path}")
    print(f"Escribiendo: {output_path}")

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
            # Limpiar espacios en blanco sobrantes por fila
            # (no reemplazar NA — dbt los convierte a NULL con NULLIF)
            writer.writerow([field.strip() for field in row])
            rows_written += 1

    print(f"Filas escritas (incluye encabezado): {rows_written}")
    print("Listo.")


def main():
    parser = argparse.ArgumentParser(description="Convierte CSV Latin-1 a UTF-8")
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Ruta al CSV original (Latin-1)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Ruta del CSV de salida (UTF-8). Por defecto: mismo dir con sufijo _utf8",
    )
    args = parser.parse_args()

    output = args.output or args.input.with_stem(args.input.stem + "_utf8")
    convert(args.input, output)


if __name__ == "__main__":
    main()
