-- deploy/postgres/init/01_schemas.sql
-- Schema namespaces only (can be run against non-Docker Postgres too)
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS study;
CREATE SCHEMA IF NOT EXISTS atmotube;
CREATE SCHEMA IF NOT EXISTS fitbit;
-- CREATE SCHEMA IF NOT EXISTS timeline; -- google maps timeline data
-- CREATE SCHEMA IF NOT EXISTS whatsapp; -- whatsapp data
