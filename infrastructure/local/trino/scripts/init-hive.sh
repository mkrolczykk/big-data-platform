#!/bin/bash

# Create necessary directories
mkdir -p /home/hive/.beeline

echo "=== Hive Metastore Startup ==="
echo "Java version:"
java -version

echo "Waiting for MySQL to be ready..."
until nc -z mysql_db 3306; do
  echo "Waiting for MySQL..."
  sleep 2
done

echo "MySQL is ready!"
sleep 10

echo "Checking Hive Metastore schema status..."

# SPRAWDŹ CZY SCHEMAT JUŻ ISTNIEJE POPRZEZ BEZPOŚREDNIE ZAPYTANIE DO BAZY
if mysql -h mysql_db -u hive -phive hive_metastore -e "SELECT COUNT(*) FROM CTLGS" 2>/dev/null; then
    echo "Hive Metastore schema already exists (CTLGS table found)."
    echo "Checking if schema is complete..."

    REQUIRED_TABLES=("TBLS" "DBS" "CTLGS" "SDS" "CDS" "COLUMNS_V2")
    MISSING_TABLES=()

    for table in "${REQUIRED_TABLES[@]}"; do
        if mysql -h mysql_db -u hive -phive hive_metastore -e "SELECT 1 FROM $table LIMIT 1" 2>/dev/null; then
            echo "Table $table exists"
        else
            echo "Table $table missing or error accessing it"
            MISSING_TABLES+=("$table")
        fi
    done

    if [ ${#MISSING_TABLES[@]} -eq 0 ]; then
        echo "Hive schema is complete. Skipping initialization."
    else
        echo "Hive schema is incomplete. Missing tables: ${MISSING_TABLES[*]}"
        echo "Attempting to repair schema..."

        /opt/hive/bin/schematool -info -dbType mysql
        if [ $? -ne 0 ]; then
            echo "Schema is inconsistent. You may need to manually repair or recreate the database."
            echo "Starting metastore anyway..."
        fi
    fi
else
    echo "Hive Metastore schema does not exist or CTLGS table missing."
    echo "Initializing Hive Metastore Schema..."

    set +e
    for i in {1..3}; do
        echo "Initialization attempt $i of 3..."
        /opt/hive/bin/schematool -initSchema -dbType mysql -verbose

        INIT_RESULT=$?
        if [ $INIT_RESULT -eq 0 ]; then
            echo "Schema initialized successfully on attempt $i"
            break
        else
            echo "Schema initialization failed on attempt $i with code: $INIT_RESULT"

            if mysql -h mysql_db -u hive -phive hive_metastore -e "SELECT COUNT(*) FROM TBLS" 2>/dev/null; then
                echo "Key tables exist despite initialization error. Considering initialization successful."
                break
            fi

            if [ $i -lt 3 ]; then
                echo "Waiting 10 seconds before retry..."
                sleep 10
            else
                echo "All initialization attempts failed."
                if mysql -h mysql_db -u hive -phive hive_metastore -e "SELECT COUNT(*) FROM TBLS" 2>/dev/null; then
                    echo "But key tables exist! Starting metastore anyway..."
                else
                    echo "Schema does not exist. You may need to manually drop and recreate the database."
#                    exit 1 # tmp solution
                fi
            fi
        fi
    done
    set -e
fi

echo "Starting Hive Metastore..."
exec /opt/hive/bin/hive --service metastore