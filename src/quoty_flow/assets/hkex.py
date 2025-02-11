from dagster import (
    asset,
    AssetIn,
    Output,
    MetadataValue,
    AssetExecutionContext,
    asset_check,
)
import pandas as pd
from typing import List, Dict, Any
from ..resources.hkex import HKEXScraperResource  # 导入资源类


@asset(
    required_resource_keys={"hkex_scraper"},
    description="Scrapes bond data from HKEX website",
)
def raw_bond_data(context: AssetExecutionContext) -> Output[List[Dict[str, Any]]]:
    """从 HKEX 网站爬取债券数据"""
    scraper: HKEXScraperResource = context.resources.hkex_scraper
    data = scraper.get_bond_data()

    return Output(
        data,
        metadata={
            "record_count": len(data),
            "sample_data": MetadataValue.json(data[:5]),
        },
    )


@asset(
    required_resource_keys={"delta_io"},
    description="Processes and writes bond data to staging",
    ins={"raw_data": AssetIn("raw_bond_data")},
)
def staged_bond_data(
    context: AssetExecutionContext, raw_data: List[Dict[str, Any]]
) -> pd.DataFrame:
    """处理并写入临时表"""
    # 转换为 DataFrame
    df = pd.DataFrame(raw_data)

    # 数据清洗和转换
    df["payment_date"] = pd.to_datetime(df["payment_date"])
    df["determination_date"] = pd.to_datetime(df["determination_date"])
    df["interest_rate"] = df["interest_rate"].str.rstrip("%").astype(float)

    # 写入临时表
    context.resources.delta_io.write_table(
        df, table_name="hkex_bonds_staging", mode="overwrite"
    )

    return df


@asset_check(asset=staged_bond_data)
def check_bond_data_quality(context, df: pd.DataFrame) -> None:
    """验证临时表数据质量"""
    # 检查缺失值
    missing_values = df.isnull().sum()
    if missing_values.any():
        context.fail(
            description="Found missing values in data",
            metadata={"missing_values": missing_values.to_dict()},
        )

    # 检查股票代码格式
    invalid_symbols = df[~df["symbol"].str.match(r"^\d{4}$")]["symbol"].tolist()
    if invalid_symbols:
        context.fail(
            description="Found invalid symbols",
            metadata={"invalid_symbols": invalid_symbols},
        )

    # 检查利率范围
    invalid_rates = df[(df["interest_rate"] < 0) | (df["interest_rate"] > 100)][
        "symbol"
    ].tolist()
    if invalid_rates:
        context.fail(
            description="Found invalid interest rates",
            metadata={"invalid_rates": invalid_rates},
        )

    # 检查日期有效性
    invalid_dates = df[
        (df["payment_date"] < "2000-01-01") | (df["determination_date"] < "2000-01-01")
    ]["symbol"].tolist()
    if invalid_dates:
        context.fail(
            description="Found invalid dates", metadata={"invalid_dates": invalid_dates}
        )

    # 记录成功的检查结果
    context.add_metadata(
        metadata={
            "total_records": len(df),
            "unique_symbols": len(df["symbol"].unique()),
            "date_range": {
                "min": df["payment_date"].min().isoformat(),
                "max": df["payment_date"].max().isoformat(),
            },
        }
    )


@asset(
    required_resource_keys={"delta_io"},
    description="Publishes validated bond data",
    ins={"staged_data": AssetIn("staged_bond_data")},
)
def published_bond_data(
    context: AssetExecutionContext, staged_data: pd.DataFrame
) -> None:
    """发布已验证的数据到正式表"""
    context.resources.delta_io.write_table(
        staged_data, table_name="hkex_bonds", mode="overwrite"
    )


@asset(
    required_resource_keys={"motherduck"},
    description="Fetches raw IPO data from MotherDuck",
)
def raw_ipo_data(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """从 MotherDuck 获取原始 IPO 数据"""
    query = "SELECT * FROM main.raw_ipo"
    df = context.resources.motherduck.execute_query(query)

    return Output(
        df,
        metadata={
            "record_count": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        },
    )


@asset(
    required_resource_keys={"delta_io"},
    description="Processes and writes IPO data to staging",
    ins={"raw_data": AssetIn("raw_ipo_data")},
)
def staged_ipo_data(
    context: AssetExecutionContext, raw_data: pd.DataFrame
) -> pd.DataFrame:
    """处理并写入临时表"""
    # 数据清洗和转换
    df = raw_data.copy()
    df["ticker"] = df["ticker"].str.lstrip("0")

    # 写入临时表
    context.resources.delta_io.write_table(
        df, table_name="ipo_staging", mode="overwrite"
    )

    return df


@asset_check(asset=staged_ipo_data)
def check_ipo_data_quality(context, df: pd.DataFrame) -> None:
    """验证临时表数据质量"""
    # 检查缺失值
    missing_values = df.isnull().sum()
    if missing_values.any():
        context.fail(
            description="Found missing values in data",
            metadata={"missing_values": missing_values.to_dict()},
        )

    # 检查 ticker 格式（必须是数字字符串）
    invalid_tickers = df[~df["ticker"].str.match(r"^\d+$")]["ticker"].tolist()
    if invalid_tickers:
        context.fail(
            description="Found invalid ticker formats",
            metadata={"invalid_tickers": invalid_tickers},
        )

    # 记录成功的检查结果
    context.add_metadata(
        metadata={
            "total_records": len(df),
            "unique_tickers": len(df["ticker"].unique()),
            "ticker_length_stats": df["ticker"].str.len().describe().to_dict(),
        }
    )


@asset(
    required_resource_keys={"delta_io"},
    description="Publishes validated IPO data",
    ins={"staged_data": AssetIn("staged_ipo_data")},
)
def published_ipo_data(
    context: AssetExecutionContext, staged_data: pd.DataFrame
) -> None:
    """发布已验证的数据到正式表"""
    context.resources.delta_io.write_table(
        staged_data, table_name="ipo", mode="overwrite"
    )
