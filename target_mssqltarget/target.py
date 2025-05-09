"""mssqltarget target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_mssqltarget.sinks import (
    mssqltargetSink,
)


class Targetmssqltarget(SQLTarget):
    """Sample target for mssqltarget."""

    name = "target-mssqltarget"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,  # Flag config as protected.
            title="SQLAlchemy URL",
            description="SQLAlchemy connection string",
        ),
        th.Property(
            "username",
            th.StringType,
            description="SQL Server username",
        ),
        th.Property(
            "password",
            th.StringType,
            description="SQL Server password",
        ),
        th.Property(
            "host",
            th.StringType,
            description="SQL Server host",
        ),
        th.Property(
            "port",
            th.StringType,
            default="1433",
            description="SQL Server port",
        ),
        th.Property(
            "database",
            th.StringType,
            description="SQL Server database",
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            description="Default target schema to write to",
        ),
        th.Property(
            "table_prefix", th.StringType, description="Prefix to add to table name"
        ),
        th.Property(
            "prefer_float_over_numeric",
            th.BooleanType,
            description="Use float data type for numbers (otherwise number type is used)",
            default=False,
        ),
    ).to_dict()

    default_sink_class = mssqltargetSink


if __name__ == "__main__":
    Targetmssqltarget.cli()
