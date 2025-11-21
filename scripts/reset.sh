#!/bin/bash

# Find the Postgres container name (matches *_db_1 or db-1)
DB_CONTAINER=$(docker ps --format "{{.Names}}" | grep -E "db|postgres" | head -n 1)

if [ -z "$DB_CONTAINER" ]; then
    echo "❌ No Postgres container found. Is docker compose up running?"
    exit 1
fi

echo "⚙️ Running reset.sql inside container: $DB_CONTAINER"

docker exec -i "$DB_CONTAINER" psql -U postgres -d shopzada < ./sql/reset.sql

echo "✅ Database reset completed."
