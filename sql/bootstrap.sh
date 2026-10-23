#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<SQL
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
CREATE USER movies WITH PASSWORD '${POSTGRES_PASSWORD}';
CREATE DATABASE movies OWNER movies;
SQL
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname movies -f /docker-entrypoint-initdb.d/init.sql
