from dagster import IOManager
from duckdb import connect
from typing import Mapping
import pandas as pd
from dagster import DuckdbPandasIOManager


class SQL:
    def __init__(self, sql, **bindings):
        self.sql = sql
        self.bindings = bindings


def collect_dataframes(s: SQL) -> Mapping[str, pd.DataFrame]:
    dataframes = {}
    for key, value in s.bindings.items():
        if isinstance(value, pd.DataFrame):
            dataframes[f"df_{id(value)}"] = value
        elif isinstance(value, SQL):
            dataframes.update(collect_dataframes(value))
    return dataframes


class DuckDB:
    def __init__(self, options="", url=":memory:"):
        self.options = options
        self.url = url

    def query(self, select_statement: SQL):
        db = connect(self.url)
        db.query("install httpfs; load httpfs;")
        db.query(self.options)

        result = db.query(select_statement.sql)
        if result is None:
            return
        return result.df()


class MotherduckIOManager(IOManager):
    def __init__(self, duckdb: DuckDB, prefix=""):
        self.duckdb = duckdb
        self.prefix = prefix

    def _get_table_name(self, context):
        if context.has_asset_key:
            id = context.get_asset_identifier()
        else:
            id = context.get_identifier()
        return f"{self.prefix}{'_'.join(id)}"

    def handle_output(self, context, select_statement: SQL):
        if select_statement is None:
            return

        if not isinstance(select_statement, SQL):
            raise ValueError(
                f"Expected asset to return a SQL; got {select_statement!r}"
            )

        self.duckdb.query(
            SQL(
                "create or replace table $table_name as $select_statement",
                table_name=self._get_table_name(context),
                select_statement=select_statement,
            )
        )

    def load_input(self, context) -> SQL:
        return SQL(
            "select * from $table_name", table_name=self._get_table_name(context)
        )
