-- deploy/postgres/init/00_extensions.sql
CREATE EXTENSION IF NOT EXISTS postgis;
SELECT postgis_full_version(); -- for debugging if PostGIs doesn't load properly  
CREATE EXTENSION IF NOT EXISTS btree_gist;
