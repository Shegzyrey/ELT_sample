#!/bin/bash

set -e

host="olap-db"
port="5432"
timeout=30

echo "Waiting for $host:$port to be ready..."

# Wait for the PostgreSQL port to be open
wait_for_pg() {
    local start_time=$(date +%s)
    while true; do
        (nc -z "$host" "$port" && break) || true
        sleep 1
        local current_time=$(date +%s)
        local elapsed_time=$((current_time - start_time))
        if [ "$elapsed_time" -ge "$timeout" ]; then
            echo "Timeout reached. Unable to connect to $host:$port."
            exit 1
        fi
    done
}

wait_for_pg

echo "$host:$port is ready. Executing olap-star-schema.sql..."

# Execute the OLAP schema script
psql -h "$host" -U postgres -d postgres -f /docker-entrypoint-initdb.d/olap-star-schema.sql

echo "OLAP schema initialization completed."

exec "$@"
