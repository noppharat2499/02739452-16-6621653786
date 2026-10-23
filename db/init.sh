#!/bin/bash
# Runs once, on first start of the postgres container.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres -c "CREATE DATABASE media;"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname media -f /schema.sql
