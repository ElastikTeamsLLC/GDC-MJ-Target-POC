"""MSSQLtarget target sink class, which handles writing streams."""

from __future__ import annotations
import json
import re
from typing import Any, Dict, Iterable, List, Optional
from datetime import datetime

from singer_sdk import PluginBase
from singer_sdk.connectors import SQLConnector
from singer_sdk.helpers._conformers import replace_leading_digit
from singer_sdk.sinks import SQLSink
from sqlalchemy import Column, text
import sqlalchemy
import threading
from contextlib import contextmanager
from sqlalchemy import text

from target_mssqltarget.connector import mssqltargetConnector

class mssqltargetSink(SQLSink):
    """MSSQLtarget target sink class."""

    connector_class = mssqltargetConnector

    def __init__(
            self,
            target: PluginBase,
            stream_name: str,
            schema: dict,
            key_properties: list[str] | None,
            connector: SQLConnector | None = None,
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties, connector)
        if self._config.get("table_prefix"):
            self.stream_name = self._config.get("table_prefix") + stream_name

    @property
    def connector(self) -> mssqltargetConnector:
        """The connector object."""
        if not hasattr(self, '_connector'):
            self._connector = self.get_connector()
        return self._connector

    def get_connector(self) -> mssqltargetConnector:
        """Get a new connector instance."""
        return mssqltargetConnector(dict(self.config))

    @property
    def schema_name(self) -> Optional[str]:
        """Return the schema name or None if using a default schema."""
        default_target_schema = self.config.get("default_target_schema", None)
        parts = self.stream_name.split("-")

        if default_target_schema:
            return default_target_schema

        if len(parts) in {2, 3}:
            stream_schema = self.conform_name(parts[-2], "schema")
            return "dbo" if stream_schema == "public" else stream_schema

        return None
    
    

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Process incoming record and return a modified result."""
        keys = record.keys()
        for key in keys:
            if type(record[key]) in [list, dict]:
                record[key] = json.dumps(record[key], default=str)
        return record

    # def bulk_insert_records(
    #     self,
    #     full_table_name: str,
    #     schema: dict,
    #     records: Iterable[Dict[str, Any]],
    #     connection: Optional[sqlalchemy.engine.Connection] = None,
    # ) -> None:
    #     """Bulk insert records into the specified table."""
    #     if not full_table_name.startswith("#"):
    #         full_table_name = f"#{full_table_name}"
    #     insert_sql = f"INSERT INTO {full_table_name} (id, name, created_at, _sdc_source_file, _sdc_source_lineno) VALUES (:id, :name, :created_at, :_sdc_source_file, :_sdc_source_lineno)"
    #     records_list = list(records)
    #     self.logger.info(f"Inserting {len(records_list)} records into {full_table_name}")
    #     if connection:
    #         connection.execute(text(insert_sql), records_list)
    #     else:
    #         with self.connector._engine.connect() as conn:
    #             conn.execute(text(insert_sql), records_list)
    #             conn.commit()

    def try_parse_date(self, date_str, record_id):
        """Convert various date formats safely for SQL Server."""
        if not isinstance(date_str, str) or not date_str.strip():
            return None

        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y",
            "%H:%M:%S.%f",
            "%H:%M:%S",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                if fmt.startswith("%H:%M"):
                    dt = datetime.combine(datetime.today(), dt.time())

                if dt.year < 1753 or dt.year > 9999:
                    return None

                return dt
            except ValueError:
                continue

        return None



    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: Iterable[Dict[str, Any]],
        connection: Optional[sqlalchemy.engine.Connection] = None,
    ) -> None:
        """Bulk insert records into the specified table, forcing insert by disabling FK constraints efficiently."""
        self.logger.info(f"Entering bulk_insert_records for {full_table_name}")
        
        records_list = list(records)
        if not records_list:
            self.logger.info("No records to insert.")
            return

        self.logger.info(f"Processing {len(records_list)} records")
        
        # Get table columns and their nullability
        table_name = full_table_name.split('.')[-1]
        with self.connector._engine.connect() as conn:
            # Get column info
            result = conn.execute(
                text(f"SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
            ).fetchall()
            table_columns_info = {row[0].lower(): {'nullable': row[1] == 'YES', 'data_type': row[2]} for row in result}
            self.logger.info(f"Table columns info: {table_columns_info}")

            # Get foreign key info
            self.logger.debug(f"Full table name for FK query: {str(full_table_name)}")
            fk_query = text("""
                SELECT 
                    c.name AS column_name,
                    t.name AS referenced_table,
                    rc.name AS referenced_column,
                    fk_obj.name AS constraint_name
                FROM sys.foreign_key_columns fk
                JOIN sys.columns c ON fk.parent_object_id = c.object_id AND fk.parent_column_id = c.column_id
                JOIN sys.tables t ON fk.referenced_object_id = t.object_id
                JOIN sys.columns rc ON fk.referenced_object_id = rc.object_id AND fk.referenced_column_id = rc.column_id
                JOIN sys.foreign_keys fk_obj ON fk.constraint_object_id = fk_obj.object_id
                WHERE fk.parent_object_id = OBJECT_ID(:table_name)
            """)
            fk_result = conn.execute(fk_query, {"table_name": str(full_table_name)}).fetchall()
            fk_info = {row[0].lower(): {'table': row[1], 'column': row[2], 'constraint': row[3]} for row in fk_result}
            self.logger.info(f"Foreign key info: {fk_info}")

        # Filter schema columns to match table columns
        schema_columns = [self.conform_name(col, "column") for col in schema["properties"]]
        columns = [col for col in schema_columns if col in table_columns_info]
        
        # Check if 'id' should be included based on data presence
        has_id_values = any(record.get('ID') is not None for record in records_list)
        if not has_id_values and 'id' in columns:
            self.logger.info("No 'id' values provided in records; excluding 'id' from INSERT to use IDENTITY")
            columns.remove('id')

        self.logger.info(f"Filtered columns for INSERT: {columns}")
        params = [f":{col}" for col in columns]
        insert_sql = f"INSERT INTO {full_table_name} ({', '.join(columns)}) VALUES ({', '.join(params)})"
        self.logger.info(f"Generated INSERT SQL: {insert_sql}")

        # Transform records, applying defaults for NOT NULL columns
        self.logger.info("Transforming records...")
        transformed_records = []
        for i, record in enumerate(records_list):
            transformed_record = {}
            for schema_col in schema["properties"]:
                conformed_col = self.conform_name(schema_col, "column")
                if conformed_col in columns:  # Only include columns used in the INSERT
                    value = record.get(schema_col, None)
                    if value is None and not table_columns_info[conformed_col]['nullable']:
                        data_type = table_columns_info[conformed_col]['data_type']
                        if data_type in ('int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric'):
                            default_value = 0
                        elif data_type in ('nvarchar', 'varchar', 'char', 'nchar', 'text'):
                            default_value = ''
                        elif data_type in ('datetime', 'smalldatetime', 'date'):
                            default_value = '1900-01-01'
                        else:
                            default_value = 0  # Fallback
                        self.logger.warning(f"Record {i+1} has missing {schema_col} (column {conformed_col}); defaulting to {default_value}")
                        value = default_value
                    if schema_col == 'ID':
                        self.logger.debug(f"Record {i+1} ID value: {value}")
                    transformed_record[conformed_col] = value
            transformed_records.append(transformed_record)
        self.logger.info(f"Transformed {len(transformed_records)} records")

        # Execute insert with optimized FK constraint handling
        self.logger.info("Executing bulk insert with forced constraints...")
        try:
            with self.connector._engine.connect() as conn:
                # Check for blocking processes
                lock_check = text("""
                    SELECT blocking_session_id, wait_time, wait_type
                    FROM sys.dm_exec_requests
                    WHERE session_id IN (
                        SELECT session_id
                        FROM sys.dm_tran_locks
                        WHERE resource_database_id = DB_ID()
                        AND resource_associated_entity_id = OBJECT_ID(:table_name)
                    )
                    AND blocking_session_id != 0
                """)
                locks = conn.execute(lock_check, {"table_name": str(full_table_name)}).fetchall()
                if locks:
                    self.logger.warning(f"Detected blocking processes: {locks}")

                # Batch disable all FK constraints in one statement
                if fk_info:
                    disable_sql = "ALTER TABLE {} NOCHECK CONSTRAINT {}".format(
                        full_table_name,
                        ", ".join(info['constraint'] for info in fk_info.values())
                    )
                    self.logger.info(f"Disabling FK constraints in batch: {disable_sql}")
                    conn.execute(text(disable_sql))
                else:
                    self.logger.info("No FK constraints to disable.")

                # Perform the insert
                if self.key_properties and 'id' in columns:
                    self.logger.info(f"Enabling IDENTITY_INSERT for {full_table_name}")
                    conn.execute(text(f"SET IDENTITY_INSERT {full_table_name} ON"))
                    self.logger.debug(f"Connection state before execute: {conn.closed}")
                    conn.execute(text(insert_sql), transformed_records)
                    self.logger.info(f"Disabling IDENTITY_INSERT for {full_table_name}")
                    conn.execute(text(f"SET IDENTITY_INSERT {full_table_name} OFF"))
                else:
                    self.logger.debug(f"Connection state before execute: {conn.closed}")
                    conn.execute(text(insert_sql), transformed_records)

                # Batch re-enable all FK constraints in one statement
                if fk_info:
                    enable_sql = "ALTER TABLE {} CHECK CONSTRAINT {}".format(
                        full_table_name,
                        ", ".join(info['constraint'] for info in fk_info.values())
                    )
                    self.logger.info(f"Re-enabling FK constraints in batch: {enable_sql}")
                    conn.execute(text(enable_sql))
                else:
                    self.logger.info("No FK constraints to re-enable.")

                conn.commit()
            self.logger.info(f"Successfully inserted {len(transformed_records)} records into {full_table_name} with constraints forced.")
        except sqlalchemy.exc.OperationalError as e:
            self.logger.error(f"Failed to insert records into {full_table_name}. Error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during bulk insert: {str(e)}")
            raise



    # def process_batch(self, context: dict) -> None:
    #     """Process a batch of records."""
    #     conformed_records = (
    #         [self.conform_record(record) for record in context["records"]]
    #         if isinstance(context["records"], list)
    #         else (self.conform_record(record) for record in context["records"])
    #     )
    #     join_keys = [self.conform_name(key, "column") for key in self.key_properties]
    #     schema = self.conform_schema(self.schema)

    #     if self.key_properties:
    #         self.logger.info(f"Preparing permanent table {self.full_table_name}")
    #         self.connector.prepare_table(
    #             full_table_name=self.full_table_name,
    #             schema=schema,
    #             primary_keys=join_keys,
    #             as_temp_table=False,
    #         )
    #         tmp_table_name = f"#{self.full_table_name.split('.')[-1]}"

    #         with self.connector._engine.connect() as conn:
    #             self.logger.info(f"Preparing temp table {tmp_table_name}")
    #             self.connector.prepare_table(
    #                 full_table_name=tmp_table_name,
    #                 schema=schema,
    #                 primary_keys=join_keys,
    #                 as_temp_table=True,
    #                 connection=conn,
    #             )
    #             self.logger.info(f"Created temp table {tmp_table_name}")

    #             self.bulk_insert_records(
    #                 full_table_name=tmp_table_name,
    #                 schema=schema,
    #                 records=conformed_records,
    #                 connection=conn,
    #             )

    #             #Check temp table contents before merge
    #             temp_count = conn.execute(text(f"SELECT COUNT(*) FROM {tmp_table_name}")).scalar()
    #             self.logger.info(f"Records in {tmp_table_name} before merge: {temp_count}")

    #             self.logger.info(f"Merging data from temp table to {self.full_table_name}")
    #             self.merge_upsert_from_table(
    #                 from_table_name=tmp_table_name,
    #                 to_table_name=self.full_table_name,
    #                 schema=schema,
    #                 join_keys=join_keys,
    #                 connection=conn,
    #             )

    #             # Check permanent table contents after merge
    #             perm_count = conn.execute(text(f"SELECT COUNT(*) FROM {self.full_table_name}")).scalar()
    #             self.logger.info(f"Records in {self.full_table_name} after merge: {perm_count}")

    #             # Drop the temp table 
    #             conn.execute(text(f"DROP TABLE {tmp_table_name}"))
    #             self.logger.info(f"Dropped temp table {tmp_table_name}")

    #             # Explicitly commit the transaction
    #             conn.commit()


    #         with self.connector._engine.connect() as conn:
    #             final_count = conn.execute(text(f"SELECT COUNT(*) FROM {self.full_table_name}")).scalar()
    #             self.logger.info(f"Records in {self.full_table_name} after commit: {final_count}")

    #     else:
    #         self.bulk_insert_records(
    #             full_table_name=self.full_table_name,
    #             schema=schema,
    #             records=conformed_records,
    #         )

    #     # Log metrics 
    #     record_count = len(list(conformed_records))
    #     self.logger.info(f"Processed {record_count} records for stream {self.stream_name}")

    def process_batch(self, context: dict) -> None:
        """Process a batch of records, forcing insert by handling all dependencies."""
        conformed_records = (
            [self.conform_record(record) for record in context["records"]]
            if isinstance(context["records"], list)
            else (self.conform_record(record) for record in context["records"])
        )
        join_keys = [self.conform_name(key, "column") for key in self.key_properties]
        schema = dict(self.schema)

        self.logger.info(f"Schema before processing: {schema}")
        self.logger.info(f"Preparing permanent table {self.full_table_name}")
        self.connector.prepare_table(
            full_table_name=self.full_table_name,
            schema=schema,
            primary_keys=join_keys,
            as_temp_table=False,
        )

        with self.connector._engine.connect() as conn:
            table_name = self.full_table_name.split('.')[-1]
            result = conn.execute(
                text(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
            ).fetchall()
            table_schema = {row[0].lower(): row[1] for row in result}
            self.logger.info(f"Table schema before alter: {table_schema}")

            for field, props in schema["properties"].items():
                field_type = props["type"] if isinstance(props["type"], str) else props["type"][0]
                conformed_field = self.conform_name(field, "column")
                if conformed_field in table_schema:
                    current_type = table_schema[conformed_field]
                    if field_type == "string" and current_type in ("int", "bigint", "smallint", "tinyint"):
                        fk_query = text("""
                            SELECT 
                                fk.name AS constraint_name,
                                OBJECT_NAME(fk.parent_object_id) AS table_name,
                                OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
                                pc.name AS column_name,
                                rc.name AS referenced_column
                            FROM sys.foreign_keys fk
                            JOIN sys.foreign_key_columns fkc 
                                ON fk.object_id = fkc.constraint_object_id
                            JOIN sys.columns pc 
                                ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
                            JOIN sys.columns rc 
                                ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
                            WHERE fk.parent_object_id = OBJECT_ID(:table_name)
                            AND pc.name = :column_name
                        """)
                        table_name_str = str(self.full_table_name)
                        fk_result = conn.execute(
                            fk_query,
                            {"table_name": table_name_str, "column_name": conformed_field}
                        ).fetchall()

                        for fk in fk_result:
                            constraint_name = fk[0]
                            self.logger.info(f"Dropping foreign key constraint {constraint_name} on {conformed_field}")
                            conn.execute(
                                text(f"ALTER TABLE {self.full_table_name} DROP CONSTRAINT {constraint_name}")
                            )

                        default_query = text("""
                            SELECT dc.name
                            FROM sys.default_constraints dc
                            JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
                            WHERE c.object_id = OBJECT_ID(:table_name) AND c.name = :column_name
                        """)
                        default_result = conn.execute(
                            default_query,
                            {"table_name": table_name_str, "column_name": conformed_field}
                        ).fetchone()
                        if default_result:
                            constraint_name = default_result[0]
                            self.logger.info(f"Dropping default constraint {constraint_name} on {conformed_field}")
                            conn.execute(text(f"ALTER TABLE {self.full_table_name} DROP CONSTRAINT {constraint_name}"))

                        computed_query = text("""
                            SELECT name
                            FROM sys.computed_columns
                            WHERE object_id = OBJECT_ID(:table_name)
                            AND definition LIKE '%' + :column_name + '%'
                        """)
                        computed_result = conn.execute(
                            computed_query,
                            {"table_name": table_name_str, "column_name": conformed_field}
                        ).fetchall()
                        for computed_col in computed_result:
                            computed_col_name = computed_col[0]
                            self.logger.info(f"Dropping computed column {computed_col_name} dependent on {conformed_field}")
                            conn.execute(
                                text(f"ALTER TABLE {self.full_table_name} DROP COLUMN {computed_col_name}")
                            )
                            if computed_col_name in [self.conform_name(c, "column") for c in schema["properties"]]:
                                self.logger.warning(f"Column {computed_col_name} dropped but present in schema; it will be excluded from insert.")

                        self.logger.info(f"Altering column {conformed_field} from {current_type} to NVARCHAR(255)")
                        conn.execute(
                            text(f"ALTER TABLE {self.full_table_name} ALTER COLUMN {conformed_field} NVARCHAR(255)")
                        )

                    elif field_type == "string" and current_type in ("datetime", "smalldatetime"):
                        default_query = text("""
                            SELECT dc.name
                            FROM sys.default_constraints dc
                            JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
                            WHERE c.object_id = OBJECT_ID(:table_name) AND c.name = :column_name
                        """)
                        default_result = conn.execute(
                            default_query,
                            {"table_name": table_name_str, "column_name": conformed_field}
                        ).fetchone()
                        if default_result:
                            constraint_name = default_result[0]
                            self.logger.info(f"Dropping default constraint {constraint_name} on {conformed_field}")
                            conn.execute(text(f"ALTER TABLE {self.full_table_name} DROP CONSTRAINT {constraint_name}"))

                        computed_query = text("""
                            SELECT name
                            FROM sys.computed_columns
                            WHERE object_id = OBJECT_ID(:table_name)
                            AND definition LIKE '%' + :column_name + '%'
                        """)
                        computed_result = conn.execute(
                            computed_query,
                            {"table_name": table_name_str, "column_name": conformed_field}
                        ).fetchall()
                        for computed_col in computed_result:
                            computed_col_name = computed_col[0]
                            self.logger.info(f"Dropping computed column {computed_col_name} dependent on {conformed_field}")
                            conn.execute(
                                text(f"ALTER TABLE {self.full_table_name} DROP COLUMN {computed_col_name}")
                            )
                            if computed_col_name in [self.conform_name(c, "column") for c in schema["properties"]]:
                                self.logger.warning(f"Column {computed_col_name} dropped but present in schema; it will be excluded from insert.")

                        self.logger.info(f"Altering column {conformed_field} from {current_type} to NVARCHAR(255)")
                        conn.execute(
                            text(f"ALTER TABLE {self.full_table_name} ALTER COLUMN {conformed_field} NVARCHAR(255)")
                        )

            result = conn.execute(
                text(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
            ).fetchall()
            table_schema_after = {row[0].lower(): row[1] for row in result}
            self.logger.info(f"Table schema after alter: {table_schema_after}")

            self.logger.info("Committing schema alterations...")
            conn.commit()
            self.logger.info("Schema alterations committed.")

        self.logger.info("Starting bulk insert...")
        try:
            self.bulk_insert_records(
                full_table_name=self.full_table_name,
                schema=schema,
                records=conformed_records,
            )
            self.logger.info("Bulk insert completed.")
        except Exception as e:
            self.logger.error(f"Bulk insert failed: {str(e)}")
            raise

        record_count = len(list(conformed_records)) if isinstance(conformed_records, list) else sum(1 for _ in conformed_records)
        self.logger.info(f"Processed {record_count} records for stream {self.stream_name}")

    def merge_upsert_from_table(
        self,
        from_table_name: str,
        to_table_name: str,
        schema: dict,
        join_keys: List[str],
        connection: Optional[sqlalchemy.engine.Connection] = None,
    ) -> Optional[int]:
        """Merge records from a temp table into the target table."""
        join_condition = " AND ".join([f"temp.{key} = target.{key}" for key in join_keys])
        update_stmt = ", ".join(
            [f"target.{key} = temp.{key}" for key in schema["properties"].keys() if key not in join_keys]
        )
        merge_sql = f"""
            MERGE INTO {to_table_name} AS target
            USING {from_table_name} AS temp
            ON {join_condition}
            WHEN MATCHED THEN
                UPDATE SET
                    {update_stmt}
            WHEN NOT MATCHED THEN
                INSERT ({", ".join(schema["properties"].keys())})
                VALUES ({", ".join([f"temp.{key}" for key in schema["properties"].keys()])});
        """
        self.logger.info(f"Executing merge SQL: {merge_sql}")
        if connection:
            result = connection.execute(text(merge_sql))
        else:
            with self.connector._engine.begin() as conn:
                result = conn.execute(text(merge_sql))
        self.logger.info(f"Merge affected {result.rowcount} rows")
        return result.rowcount