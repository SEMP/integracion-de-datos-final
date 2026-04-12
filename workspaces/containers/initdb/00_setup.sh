#!/bin/bash
# =============================================================
# 00_setup.sh — Crear bases de datos y otorgar permisos
# Se ejecuta primero (orden alfabético) para que los scripts
# .sql siguientes ya encuentren las bases creadas y accesibles
# para el usuario definido en MYSQL_USER.
# =============================================================
set -e

mysql -u root -p"${MYSQL_ROOT_PASSWORD}" <<EOF

-- Base de datos principal: accidentes DATATRAN (nombre desde MYSQL_DATABASE en .env)
CREATE DATABASE IF NOT EXISTS \`${MYSQL_DATABASE}\`
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

GRANT ALL PRIVILEGES ON \`${MYSQL_DATABASE}\`.* TO '${MYSQL_USER}'@'%';

-- Base de datos para Metabase
CREATE DATABASE IF NOT EXISTS \`${MB_DB_DBNAME:-metabase}\`
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

GRANT ALL PRIVILEGES ON \`${MB_DB_DBNAME:-metabase}\`.* TO '${MYSQL_USER}'@'%';

FLUSH PRIVILEGES;

EOF
