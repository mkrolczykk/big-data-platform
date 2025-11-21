#!/bin/bash

# Create necessary directories
mkdir -p /home/hive/.beeline

echo "=== Hive Metastore Startup ==="
echo "Java version:"
java -version

echo "Classpath check:"
find /opt/hive/lib -name "*aws*" -o -name "*s3*" | while read file; do
    echo "Found: $file"
done

echo "Waiting for MySQL to be ready..."
until nc -z mysql_db 3306; do
  echo "Waiting for MySQL..."
  sleep 2
done

echo "MySQL is ready!"

echo "Checking if Hive Metastore schema already exists..."
/opt/hive/bin/schematool -info -dbType mysql

if [ $? -eq 0 ]; then
    echo "Hive Metastore schema already exists. Skipping initialization."
else
    echo "Initializing Hive Metastore Schema..."
    /opt/hive/bin/schematool -initSchema -dbType mysql

    if [ $? -eq 0 ]; then
        echo "Schema initialized successfully"
    else
        echo "Schema initialization failed, but checking if it might already exist..."
        /opt/hive/bin/schematool -info -dbType mysql
        if [ $? -eq 0 ]; then
            echo "Schema exists despite initialization error. Continuing..."
        else
            echo "Schema does not exist and initialization failed. Exiting."
            exit 1
        fi
    fi
fi

echo "Starting Hive Metastore..."
exec /opt/hive/bin/start-metastore