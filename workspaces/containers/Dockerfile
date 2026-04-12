FROM eclipse-temurin:21-jre
ENV MB_PLUGINS_DIR=/plugins
RUN mkdir -p ${MB_PLUGINS_DIR} /app
# Metabase v0.58.8
ADD https://downloads.metabase.com/v0.58.8/metabase.jar /app/metabase.jar
# DuckDB driver
ADD https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/1.4.3.1/duckdb.metabase-driver.jar ${MB_PLUGINS_DIR}/
EXPOSE 3000
CMD ["java", "-jar", "/app/metabase.jar"]