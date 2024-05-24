#!/bin/bash
set -e
local start_time=$(date +%s)
sleep 130
het=120
local current_time=$(date +%s)
local elapsed_time=$((current_time - start_time))

if ["$elapsed_time" -gt "$het"]; then
    # Run the standard PostgreSQL entrypoint script
    /usr/local/bin/docker-entrypoint.sh "$@"



    # Run your custom script
    psql -U postgres -d olap_db -a -f /docker-entrypoint-initdb.d/olap_elt.sql
fi