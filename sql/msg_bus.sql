-- Create the msg_bus role and grant permissions to the pgmq extension
-- This is a basic example and may need to be adjusted for your specific needs

-- 1) create the database
create database msql_db if not exists msg_bus;

-- 2) create the role
create role msg_bus with login password 'REPLACE_WITH_STRONG_PASSWORD' if not exists;

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

-- 8) Grant SELECT on sequences to msg_bus
GRANT SELECT ON ALL SEQUENCES IN SCHEMA pgmq TO msg_bus;

ALTER DEFAULT PRIVILEGES IN SCHEMA pgmq
GRANT SELECT ON SEQUENCES TO msg_bus;