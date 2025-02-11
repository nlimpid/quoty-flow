from dagster import (
    AssetSpec,
    ConfigurableResource,
    MetadataValue,
)
import duckdb
import os
import pandas as pd


# 1. 定义 MotherDuck 资源
class MotherDuckResource(ConfigurableResource):
    def connect(self):
        token = os.getenv("MOTHERDUCK_TOKEN")
        return duckdb.connect(f"md:quoty?motherduck_token={token}")

    def execute_query(self, query: str, parameters: dict | None = None) -> pd.DataFrame:
        with self.connect() as conn:
            if parameters:
                result: pd.DataFrame = conn.execute(query, parameters).df()
            else:
                result: pd.DataFrame = conn.execute(query).df()
        return result


raw_ipo_table = AssetSpec(
    key="raw_ipo",  # 资产的唯一标识符
    metadata={
        "schema": "main",
        "table": "raw_ipo",
        "description": "Raw IPO data from external source",
        "columns": {
            "ticker": MetadataValue.text("Stock ticker symbol (e.g., 09988)"),
            "proposed_list_date": MetadataValue.text("Proposed listing date"),
            "issue_price": MetadataValue.text("Final IPO issue price"),
            "issue_price_min": MetadataValue.text("Minimum issue price in range"),
            "issue_price_max": MetadataValue.text("Maximum issue price in range"),
            "currency": MetadataValue.text("Price currency (e.g., HKD)"),
        },
        "refresh_frequency": "daily",
        "owner": "foo",
    },
)
