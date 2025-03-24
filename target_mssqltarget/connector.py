from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, cast
from sqlalchemy import text
from singer_sdk.connectors.sql import SQLConnector
from singer_sdk.helpers._typing import get_datelike_property_type
import sqlalchemy
from sqlalchemy.dialects import mssql

logger = logging.getLogger()

class mssqltargetConnector(SQLConnector):
    """The connector for MSSQL.
    
    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True
    allow_column_rename: bool = True
    allow_column_alter: bool = True
    allow_merge_upsert: bool = True
    allow_temp_tables: bool = True

    def table_exists(self, full_table_name: str, connection: Optional[sqlalchemy.engine.Connection] = None) -> bool:
        """Check if a table exists in the database."""
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        query = text("""
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = :schema_name AND TABLE_NAME = :table_name
        """)
        conn = connection or self._engine.connect()
        result = conn.execute(query, {"schema_name": schema_name, "table_name": table_name}).fetchone()
        return result is not None

    def prepare_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: List[str] | None = None,
        as_temp_table: bool = False,
        connection: Optional[sqlalchemy.engine.Connection] = None,
    ) -> None:
        """Prepare the table (only if it exists).

        If the table does not exist, a warning is logged and the process aborts.
        """
        if not self.table_exists(full_table_name, connection):
            logger.warning(f"Table {full_table_name} does not exist. Skipping data insertion.")
            return  # Exit without creating a table

        logger.info(f"Table {full_table_name} exists. Proceeding with data insertion.")
        # Note: Since the requirement is to not overwrite existing data,
        # we do not call create_empty_table here.

    def create_empty_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
        connection: Optional[sqlalchemy.engine.Connection] = None,
    ) -> None:
        """Create an empty table based on the provided schema."""
        logger.info(f"Creating table: {full_table_name}, as_temp_table={as_temp_table}")
        if as_temp_table and not self.allow_temp_tables:
            raise NotImplementedError("Temporary tables are not supported.")

        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        if as_temp_table:
            table_name = f"#{table_name}" if not table_name.startswith("#") else table_name
            full_table_name = table_name
        else:
            full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name

        meta = sqlalchemy.MetaData()
        columns: list[sqlalchemy.Column] = []
        primary_keys = primary_keys or []

        properties = schema.get("properties", {})
        if not properties:
            raise RuntimeError(f"Schema for '{full_table_name}' does not define properties: {schema}")

        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            columntype = self.to_sql_type(property_jsonschema)
            if isinstance(columntype, sqlalchemy.types.VARCHAR) and is_primary_key:
                columntype = sqlalchemy.types.VARCHAR(255)
            columns.append(
                sqlalchemy.Column(
                    property_name, columntype, primary_key=is_primary_key, autoincrement=False
                )
            )

        table = sqlalchemy.Table(table_name, meta, *columns, schema=schema_name if not as_temp_table else None)
        if connection:
            meta.create_all(connection)
        else:
            with self._engine.begin() as conn:
                meta.create_all(conn)
                if as_temp_table:
                    result = conn.execute(text(f"SELECT OBJECT_ID('tempdb..{table_name}')")).scalar()
                    if result is None:
                        raise RuntimeError(f"Temp table {full_table_name} not found in tempdb")
        logger.info(f"Table {full_table_name} created")
