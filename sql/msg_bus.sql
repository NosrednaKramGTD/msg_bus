-- 0) (Optional) sanity: confirm you're in the right DB
SELECT current_database();

-- 1) Create login role
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'msg_bus') THEN
    CREATE ROLE msg_bus WITH LOGIN PASSWORD 'REPLACE_WITH_STRONG_PASSWORD';
  END IF;
END $$;

-- 2) Ensure PGMQ extension is enabled in THIS database (msql_db)
-- CREATE EXTENSION loads the extension into the current database. [1](https://www.postgresql.org/docs/current/sql-createextension.html)
CREATE EXTENSION IF NOT EXISTS pgmq;

-- 3) Allow msg_bus to connect to the database
GRANT CONNECT ON DATABASE msql_db TO msg_bus;

-- 4) Grant schema access and ability to create queue tables in pgmq schema
-- PGMQ installs/uses the pgmq schema. [3](https://pgxman.com/x/pgmq)[2](https://github.com/pgmq/pgmq/blob/main/INSTALLATION.md)
GRANT USAGE, CREATE ON SCHEMA pgmq TO msg_bus;

-- 5) Allow msg_bus to call all PGMQ functions (send/read/create/etc.)
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgmq TO msg_bus;

-- 6) Full DML access on all existing queue/archive tables in pgmq schema
-- Queues are implemented as tables in pgmq schema (q_<name>, a_<name>). [3](https://pgxman.com/x/pgmq)
GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA pgmq
TO msg_bus;

-- 7) Ensure future queue/archive tables created in pgmq schema are accessible
ALTER DEFAULT PRIVILEGES IN SCHEMA pgmq
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO msg_bus;

ALTER DEFAULT PRIVILEGES IN SCHEMA pgmq
GRANT EXECUTE ON FUNCTIONS TO msg_bus;
