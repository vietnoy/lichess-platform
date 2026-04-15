-- Creates the two databases needed at container startup.
-- Postgres entrypoint runs this as the superuser before any service connects.

SELECT 'CREATE DATABASE airflow_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec
SELECT 'CREATE DATABASE polaris_db'  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'polaris_db')\gexec
