# from contextlib import closing
from typing import NewType, Any
from collections.abc import Generator
from contextlib import AbstractContextManager
import functools
import os

import fire
import aiosql
import psycopg
from psycopg.rows import dict_row
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv

load_dotenv()

CONNECTION_STRING = os.environ["CONNECTION_STRING"]

Connection = NewType("Connection", AbstractContextManager)

DUCKLAKE_TO_HIVE_DATA_TYPE = {
    "boolean": "BOOLEAN",  # True or false
    "int8": "TINYINT",  # 8-bit signed integer
    "int16": "SMALLINT",  # 16-bit signed integer
    "int32": "INT",  # 32-bit signed integer
    "int64": "BIGINT",  # 64-bit signed integer
    "uint8": "TINYINT",  # 8-bit unsigned integer
    "uint16": "SMALLINT",  # 16-bit unsigned integer
    "uint32": "INT",  # 32-bit unsigned integer
    "uint64": "BIGINT",  # 64-bit unsigned integer
    "float32": "FLOAT",  # 32-bit IEEE 754 floating-point value
    "float64": "DOUBLE",  # 64-bit IEEE 754 floating-point value
    "decimal": "DECIMAL",  # TODO: decimal(P,S): Fixed-point decimal with precision P and scale S
    "time": None,  # Time of day, microsecond precision
    "timetz": None,  # Time of day, microsecond precision, with time zone
    "date": "DATE",  # Calendar date
    "timestamp": "TIMESTAMP",  # Timestamp, microsecond precision
    "timestamptz": "TIMESTAMP",  # Timestamp, microsecond precision, with time zone
    "timestamp_s": "TIMESTAMP",  # Timestamp, second precision
    "timestamp_ms": "TIMESTAMP",  # Timestamp, millisecond precision
    "timestamp_ns": "TIMESTAMP",  # Timestamp, nanosecond precision
    "interval": None,  # Time interval in three different granularities: months, days, and milliseconds
    "varchar": "STRING",  # Text
    "blob": "BINARY",  # Binary data
    "json": "JSON",  # TODO: JSON
    "uuid": None,  # Universally unique identifier
    "list": "LIST",  # TODO: Collection of values with a single child type
    "struct": "STRUCT",  # TODO:	A tuple of typed values
    "map": "MAP",  # TODO: A collection of key-value pairs
}


def ducklake_to_hive_data_type(typename: str) -> str | None:
    try:
        return DUCKLAKE_TO_HIVE_DATA_TYPE[typename]
    except KeyError as e:
        if typename.startswith("decimal"):
            return typename
        else:
            raise e


class ParamAliases:
    sn = snapshot = "snapshot_id"
    sc = schema = "schema_id"
    tn = tname = "table_name"
    ti = tid = "table_id"


class DucklakeCatalog:
    """Execute query and export table in Ducklake catalog DB."""

    def __init__(self, queries: aiosql.queries, conn: Connection) -> None:
        self._queries = queries
        self._conn = conn

        # Dynamically adding query functions.
        for query in self._queries.available_queries:
            func = functools.partial(self._execute_query, query=query)
            func.__doc__ = getattr(self._queries, query).__doc__
            setattr(self, query, func)

    def _execute_query(self, query: str, **kwargs) -> Any:
        alias_kwargs = {
            getattr(ParamAliases, k): v
            for k, v in kwargs.items()
            if hasattr(ParamAliases, k)
        }
        return getattr(self._queries, query)(self._conn, **(kwargs | alias_kwargs))

    def table_schema_to_export(
        self, snapshot_id: int, table_id: int
    ) -> Generator[dict[str, Any]]:
        for column in self._queries.table_structure(
            self._conn, snapshot_id=snapshot_id, table_id=table_id
        ):
            column["column_type"] = ducklake_to_hive_data_type(column["column_type"])
            yield column

    def export_table(self, table_name: str, dry_run: bool = False) -> str:
        env = Environment(loader=FileSystemLoader("template/"))
        template = env.get_template("athena_table_template.sql.j2")

        snapshot_id = self._queries.current_snapshot(self._conn)
        table_id = self._queries.get_table_id(
            self._conn, snapshot_id=snapshot_id, table_name=table_name
        )
        ddl_sql = template.render(
            {
                "data_path": "s3://home-an2-dev-dp-drs-tables/",
                "schema_name": "main",
                "table_name": table_name,
                "table_comment": self._queries.get_table_comment(
                    self._conn, table_id=table_id
                ),
                "columns": self.table_schema_to_export(
                    snapshot_id=snapshot_id, table_id=table_id
                ),
            }
        )

        self._queries.save_athena_ddl(self._conn, table_id=table_id, ddl=ddl_sql)

        return ddl_sql


def main():
    queries = aiosql.from_path("sql/", "psycopg")

    with psycopg.connect(CONNECTION_STRING, row_factory=dict_row) as conn:
        ducklake_catalog = DucklakeCatalog(queries, conn)
        fire.Fire(ducklake_catalog)


if __name__ == "__main__":
    main()
