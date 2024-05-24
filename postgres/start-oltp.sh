#!/bin/bash

set -e

host="oltp-db"
port="5432"
timeout=30

echo "Waiting for $host:$port to be ready..."

# Wait for the PostgreSQL port to be open
wait_for_pg() {
    local start_time=$(date +%s)
    while true; do
        (nc -z "$host" "$port" && break) || true
        sleep 2
        local current_time=$(date +%s)
        local elapsed_time=$((current_time - start_time))
        if [ "$elapsed_time" -ge "$timeout" ]; then
            echo "Timeout reached. Unable to connect to $host:$port."
            exit 1
        fi
    done
}

# Sleep for 30 seconds before making a connection
sleep 30

# Wait for PostgreSQL to be ready
wait_for_pg

echo "$host:$port is ready. Executing oltp-schema.sql..."

# Execute the OLAP schema script
psql -h "$host" -U postgres -d oltp_db -f /docker-entrypoint-initdb.d/oltp-schema.sql

echo "OLTP schema initialization completed."

# Execute your Python script
python3 /usr/src/filler/populate_oltp_table.py

echo "Python script completed."

exec "$@"
