-- name: current_snapshot()$
-- Get Current Snapshot
--
-- Before anything else we need to find a snapshot ID to be queried.
-- There can be many snapshots in the ducklake_snapshot table.
-- A snapshot ID is a continuously increasing number that identifies a snapshot.
-- In most cases, you would query the most recent one like so.
SELECT snapshot_id
FROM ducklake_snapshot
WHERE snapshot_id =
    (SELECT max(snapshot_id) FROM ducklake_snapshot);

-- name: list_schemas(snapshot_id)
-- List Schemas
--
-- A DuckLake catalog can contain many SQL-style schemas, which each can contain many tables. 
-- These are listed in the ducklake_schema table. 
-- Here's how we get the list of valid schemas for a given snapshot.
SELECT schema_id, schema_name
FROM ducklake_schema
WHERE
    :snapshot_id >= begin_snapshot


-- name: list_tables(schema_id, snapshot_id)
-- List the tables available in a schema 
-- for a specific snapshot using the ducklake_table table
SELECT tbl.table_id, tbl.table_name, tag.value AS table_comment
FROM ducklake_table tbl
    LEFT JOIN ducklake_tag tag ON tbl.table_id = tag.object_id AND tag.key = 'comment'
WHERE tbl.schema_id = :schema_id
  AND :snapshot_id >= tbl.begin_snapshot
  AND (:snapshot_id < tbl.end_snapshot OR tbl.end_snapshot IS NULL);

-- name: get_table_id(snapshot_id, table_name)$
SELECT table_id
FROM ducklake_table 
WHERE table_name = :table_name
  AND :snapshot_id >= begin_snapshot
  AND (:snapshot_id < end_snapshot OR end_snapshot IS NULL);

-- name: get_table_comment(table_id)$
SELECT tag.value AS table_comment
FROM ducklake_tag AS tag 
WHERE tag.object_id = :table_id AND tag.key = 'comment';

-- name: table_structure(snapshot_id, table_id)
-- Show the Structure of a Table
--
-- For each given table, we can list the available top-level columns using the ducklake_column table:
SELECT c.column_id, c.column_name, c.column_type, ct.value as column_comment
FROM ducklake_column AS c
    LEFT JOIN ducklake_column_tag ct 
        ON (c.table_id = ct.table_id AND c.column_id = ct.column_id
            AND :snapshot_id >= ct.begin_snapshot 
            AND (:snapshot_id < ct.end_snapshot OR ct.end_snapshot IS NULL)
            AND ct."key" = 'comment')
WHERE
    c.table_id = :table_id
    AND c.parent_column IS NULL
    AND :snapshot_id >= c.begin_snapshot AND (:snapshot_id < c.end_snapshot OR c.end_snapshot IS NULL)
ORDER BY c.column_order;

-- name: create-table-ducklake-table-athena-ddl#
CREATE TABLE public.ducklake_table_athena_ddl
(
    id       INTEGER GENERATED ALWAYS AS IDENTITY
        CONSTRAINT ducklake_table_athena_ddl_pk
            PRIMARY KEY,
    table_id BIGINT NOT NULL
        unique,
    ddl      TEXT
);

COMMENT ON TABLE ducklake_table_athena_ddl IS 'AWS Athena 테이블 등록 DDL SQL';

ALTER TABLE ducklake_table_athena_ddl
    OWNER TO postgres;


-- name: save-athena-ddl(table_id, ddl)!
INSERT INTO ducklake_table_athena_ddl (table_id, ddl)
VALUES (:table_id, :ddl)
ON CONFLICT(table_id)
DO UPDATE SET ddl = EXCLUDED.ddl;
