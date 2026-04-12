#!/bin/bash
set -e

mysql -u root -p"${MYSQL_ROOT_PASSWORD}" <<EOF
CREATE DATABASE IF NOT EXISTS ${MB_DB_DBNAME:-metabase}
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

GRANT ALL PRIVILEGES ON \`${MB_DB_DBNAME:-metabase}\`.* TO '${MYSQL_USER}'@'%';
FLUSH PRIVILEGES;
EOF
