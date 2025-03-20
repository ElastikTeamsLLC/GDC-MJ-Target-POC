"""mssqltarget target sink class, which handles writing streams."""

from __future__ import annotations
import json
import re
from typing import Any, Dict, Iterable, List, Optional

from singer_sdk import PluginBase
from singer_sdk.connectors import SQLConnector
from singer_sdk.helpers._conformers import replace_leading_digit
from singer_sdk.sinks import SQLSink
from sqlalchemy import Column, text
import sqlalchemy

from target_mssqltarget.connector import mssqltargetConnector


# class mssqltargetConnector(SQLConnector):
#     """The connector for mssqltarget.

#     This class handles all DDL and type conversions.
#     """

#     allow_column_add: bool = True  # Whether ADD COLUMN is supported.
#     allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
#     # Whether altering column types is supported.
#     allow_column_alter: bool = False
#     allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
#     # Whether overwrite load method is supported.
#     allow_overwrite: bool = False
#     allow_temp_tables: bool = True  # Whether temp tables are supported.

#     def get_sqlalchemy_url(self, config: dict) -> str:
#         """Generates a SQLAlchemy URL for mssqltarget.

#         Args:
#             config: The configuration for the connector.
#         """
#         return super().get_sqlalchemy_url(config)


class mssqltargetSink(SQLSink):
    """mssqltarget target sink class."""

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
        """The connector object.
        
        Returns:
            The configured mssqltargetConnector instance.
        """
        if not hasattr(self, '_connector'):
            self._connector = self.get_connector()
        return self._connector

    def get_connector(self) -> mssqltargetConnector:
        """Get a new connector instance.
        
        Returns:
            A new mssqltargetConnector instance.
        """
        return mssqltargetConnector(dict(self.config))
    
    @property
    def schema_name(self) -> Optional[str]:
        """
        
        """
        default_target_schema = self.config.get("default_target_schema", None)
        parts = self.stream_name.split("-")

        if default_target_schema:
            return default_target_schema

        if len(parts) in {2, 3}:
            # Stream name is a two-part or three-part identifier.
            # Use the second-to-last part as the schema name.
            stream_schema = self.conform_name(parts[-2], "schema")

            if stream_schema == "public":
                return "dbo"
            else:
                return stream_schema

        # Schema name not detected.
        return None

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Process incoming record and return a modified result.
        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        Returns:
            A new, processed record.
        """
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
    #     is_temp_table: bool = False,
    # ) -> Optional[int]:
    #     """Bulk insert records to an existing destination table.
    #     The default implementation uses a generic SQLAlchemy bulk insert operation.
    #     This method may optionally be overridden by developers in order to provide
    #     faster, native bulk uploads.
    #     Args:
    #         full_table_name: the target table name.
    #         schema: the JSON schema for the new table, to be used when inferring column
    #             names.
    #         records: the input records.
    #         is_temp_table: whether the table is a temp table.
    #     Returns:
    #         True if table exists, False if not, None if unsure or undetectable.
    #     """
    #     insert_sql = self.generate_insert_statement(
    #         full_table_name,
    #         schema,
    #     )
    #     if isinstance(insert_sql, str):
    #         insert_sql = sqlalchemy.text(insert_sql)

    #     self.logger.info("Inserting with SQL: %s", insert_sql)

    #     columns = self.column_representation(schema)

    #     # temporary fix to ensure missing properties are added
    #     insert_records = []
    #     for record in records:
    #         insert_record = {}
    #         for column in columns:
    #             insert_record[column.name] = record.get(column.name)
    #         insert_records.append(insert_record)

    #     self.connection.execute(insert_sql, insert_records)

    #     if isinstance(records, list):
    #         return len(records)  # If list, we can quickly return record count.

    #     return None  # Unknown record count.
    
    def bulk_insert_records(
    self,
        full_table_name: str,
        schema: dict,
        records: Iterable[Dict[str, Any]],
        connection: Optional[sqlalchemy.engine.Connection] = None,
    ) -> None:
        if not full_table_name.startswith("#"):
            full_table_name = f"#{full_table_name}"
        insert_sql = f"INSERT INTO {full_table_name} (id, name, created_at, _sdc_source_file, _sdc_source_lineno) VALUES (:id, :name, :created_at, :_sdc_source_file, :_sdc_source_lineno)"
        if connection:
            connection.execute(text(insert_sql), list(records))
        else:
            with self.connector._engine.connect() as conn:
                conn.execute(text(insert_sql), list(records))
                conn.commit()

    def column_representation(
        self,
        schema: dict,
    ) -> List[Column]:
        """Returns a sql alchemy table representation for the current schema."""
        columns: list[Column] = []
        conformed_properties = self.conform_schema(schema)["properties"]
        for property_name, property_jsonschema in conformed_properties.items():
            columns.append(
                Column(
                    property_name,
                    self.connector.to_sql_type(property_jsonschema),
                )
            )
        return columns

    # def process_batch(self, context: dict) -> None:
    #     """Process a batch with the given batch context.
    #     Writes a batch to the SQL target. Developers may override this method
    #     in order to provide a more efficient upload/upsert process.
    #     Args:
    #         context: Stream partition or context dictionary.
    #     """
    #     # First we need to be sure the main table is already created
    #     conformed_records = (
    #         [self.conform_record(record) for record in context["records"]]
    #         if isinstance(context["records"], list)
    #         else (self.conform_record(record) for record in context["records"])
    #     )

    #     join_keys = [self.conform_name(key, "column") for key in self.key_properties]
    #     schema = self.conform_schema(self.schema)

    #     if self.key_properties:
    #         self.logger.info(f"Preparing table {self.full_table_name}")
    #         self.connector.prepare_table(
    #             full_table_name=self.full_table_name,
    #             schema=schema,
    #             primary_keys=join_keys,
    #             as_temp_table=False,
    #         )
    #         # Create a temp table (Creates from the table above)
    #         self.logger.info(f"Creating temp table {self.full_table_name}")
    #         self.connector.create_temp_table_from_table(
    #             from_table_name=self.full_table_name
    #         )

    #         db_name, schema_name, table_name = self.parse_full_table_name(
    #             self.full_table_name
    #         )
    #         tmp_table_name = (
    #             f"{schema_name}.#{table_name}" if schema_name else f"#{table_name}"
    #         )
    #         # Insert into temp table
    #         self.bulk_insert_records(
    #             full_table_name=tmp_table_name,
    #             schema=schema,
    #             records=conformed_records,
    #         )
    #         # Merge data from Temp table to main table
    #         self.logger.info(f"Merging data from temp table to {self.full_table_name}")
    #         self.merge_upsert_from_table(
    #             from_table_name=tmp_table_name,
    #             to_table_name=self.full_table_name,
    #             schema=schema,
    #             join_keys=join_keys,
    #         )

    #     else:
    #         self.bulk_insert_records(
    #             full_table_name=self.full_table_name,
    #             schema=schema,
    #             records=conformed_records,
    #         )

    def process_batch(self, context: dict) -> None:
        conformed_records = (
            [self.conform_record(record) for record in context["records"]]
            if isinstance(context["records"], list)
            else (self.conform_record(record) for record in context["records"])
        )
        join_keys = [self.conform_name(key, "column") for key in self.key_properties]
        schema = self.conform_schema(self.schema)

        if self.key_properties:
            self.logger.info(f"Preparing permanent table {self.full_table_name}")
            self.connector.prepare_table(
                full_table_name=self.full_table_name,
                schema=schema,
                primary_keys=join_keys,
                as_temp_table=False,
            )
            tmp_table_name = f"#{self.full_table_name.split('.')[-1]}"  # e.g., #test_stream

            # Use a single connection for all operations
            with self.connector._engine.connect() as conn:
                # Create temp table within the connection
                self.logger.info(f"Preparing temp table {tmp_table_name}")
                self.connector.prepare_table(
                    full_table_name=tmp_table_name,
                    schema=schema,
                    primary_keys=join_keys,
                    as_temp_table=True,
                    connection=conn,  # Pass the connection explicitly
                )
                self.logger.info(f"Created temp table {tmp_table_name}")

                # Insert into temp table using the same connection
                self.bulk_insert_records(
                    full_table_name=tmp_table_name,
                    schema=schema,
                    records=conformed_records,
                    connection=conn,  # Pass the connection explicitly
                )

                # Merge into permanent table using the same connection
                self.logger.info(f"Merging data from temp table to {self.full_table_name}")
                self.merge_upsert_from_table(
                    from_table_name=tmp_table_name,
                    to_table_name=self.full_table_name,
                    schema=schema,
                    join_keys=join_keys,
                    connection=conn,  # Pass the connection explicitly
                )
        else:
            self.bulk_insert_records(
                full_table_name=self.full_table_name,
                schema=schema,
                records=conformed_records,
            )

    # def merge_upsert_from_table(
    #     self,
    #     from_table_name: str,
    #     to_table_name: str,
    #     schema: dict,
    #     join_keys: List[str],
    # ) -> Optional[int]:
    #     """Merge upsert data from one table to another.
    #     Args:
    #         from_table_name: The source table name.
    #         to_table_name: The destination table name.
    #         join_keys: The merge upsert keys, or `None` to append.
    #         schema: Singer Schema message.
    #     Return:
    #         The number of records copied, if detectable, or `None` if the API does not
    #         report number of records affected/inserted.
    #     """
    #     # TODO think about sql injeciton,
    #     # issue here https://github.com/MeltanoLabs/target-postgres/issues/22

    #     join_condition = " and ".join(
    #         [f"temp.{key} = target.{key}" for key in join_keys]
    #     )

    #     update_stmt = ", ".join(
    #         [
    #             f"target.{key} = temp.{key}"
    #             for key in schema["properties"].keys()
    #             if key not in join_keys
    #         ]
    #     )  # noqa

    #     merge_sql = f"""
    #         MERGE INTO {to_table_name} AS target
    #         USING {from_table_name} AS temp
    #         ON {join_condition}
    #         WHEN MATCHED THEN
    #             UPDATE SET
    #                 { update_stmt }
    #         WHEN NOT MATCHED THEN
    #             INSERT ({", ".join(schema["properties"].keys())})
    #             VALUES ({", ".join([f"temp.{key}" for key in schema["properties"].keys()])});
    #     """  # nosec

    #     with self.connection.begin():
    #         self.connection.execute(merge_sql)

    def merge_upsert_from_table(
        self,
        from_table_name: str,
        to_table_name: str,
        schema: dict,
        join_keys: List[str],
        connection: Optional[sqlalchemy.engine.Connection] = None,
    ) -> Optional[int]:
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
        if connection:
            connection.execute(text(merge_sql))
        else:
            with self.connection.begin():
                self.connection.execute(text(merge_sql))

    def parse_full_table_name(
        self, full_table_name: str
    ) -> tuple[str | None, str | None, str]:
        """Parse a fully qualified table name into its parts.
        Developers may override this method if their platform does not support the
        traditional 3-part convention: `db_name.schema_name.table_name`
        Args:
            full_table_name: A table name or a fully qualified table name. Depending on
                SQL the platform, this could take the following forms:
                - `<db>.<schema>.<table>` (three part names)
                - `<db>.<table>` (platforms which do not use schema groupings)
                - `<schema>.<name>` (if DB name is already in context)
                - `<table>` (if DB name and schema name are already in context)
        Returns:
            A three part tuple (db_name, schema_name, table_name) with any unspecified
            or unused parts returned as None.
        """
        db_name: str | None = None
        schema_name: str | None = None

        parts = full_table_name.split(".")
        if len(parts) == 1:
            table_name = full_table_name
        if len(parts) == 2:
            schema_name, table_name = parts
        if len(parts) == 3:
            db_name, schema_name, table_name = parts

        return db_name, schema_name, table_name

    def snakecase(self, name):
        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
        return name.lower()

    def conform_name(self, name: str, object_type: Optional[str] = None) -> str:
        """Conform a stream property name to one suitable for the target system.
        Transforms names to snake case by default, applicable to most common DBMSs'.
        Developers may override this method to apply custom transformations
        to database/schema/table/column names.
        Args:
            name: Property name.
            object_type: One of ``database``, ``schema``, ``table`` or ``column``.
        Returns:
            The name transformed to snake case.
        """
        # strip non-alphanumeric characters, keeping - . _ and spaces
        name = re.sub(r"[^a-zA-Z0-9_\-\.\s]", "", name)
        # convert to snakecase
        name = self.snakecase(name)
        # replace leading digit
        return replace_leading_digit(name)