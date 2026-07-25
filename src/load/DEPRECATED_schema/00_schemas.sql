-- src/load/schemas/00_schemas.sql
-- Schema namespaces only (can be run against non-Docker Postgres too)
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS study;
CREATE SCHEMA IF NOT EXISTS atmotube;
CREATE SCHEMA IF NOT EXISTS fitbit;
-- CREATE SCHEMA IF NOT EXISTS timeline; -- google maps timeline 
-- CREATE SCHEMA IF NOT EXISTS whatsapp; -- whatsapp