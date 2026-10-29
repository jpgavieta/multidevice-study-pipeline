-- deploy/postgres/init/01_schemas.sql
-- Schema namespaces only (can be run against non-Docker Postgres too)
CREATE SCHEMA IF NOT EXISTS registry;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS readings;
CREATE SCHEMA IF NOT EXISTS accounts;

