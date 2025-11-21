#!/bin/bash

# Create necessary directories
mkdir -p /home/hive/.beeline

echo "Waiting for MySQL to be ready..."
until nc -z mysql_db 3306; do
  echo "Waiting for MySQL..."
  sleep 2
done

echo "MySQL is ready!"

# Próbuj zainicjalizować schemat, ale kontynuuj nawet jeśli się nie uda
echo "Attempting to initialize Hive Metastore Schema..."
/opt/hive/bin/schematool -initSchema -dbType mysql || true

echo "Starting Hive Metastore..."
exec /opt/hive/bin/start-metastore