from dagster import asset, AssetExecutionContext
import numpy as np
import pandas as pd
from ..models.equity_share import EquityShare
from ..resources.equity_share import OrbisfnResource
from decimal import Decimal
from ..io_managers.deltalake_io import S3DeltaResource


@asset(
    required_resource_keys={"orbisfn", "delta_io"},
    description="Orbisfn EOD 数据",
)
def orbisfn_eod(context: AssetExecutionContext) -> pd.DataFrame:
    """从 Orbisfn EOD 获取股本数据"""
    orbisfn_resource: OrbisfnResource = context.resources.orbisfn
    df: pd.DataFrame = orbisfn_resource.get_equity_shares()

    if df is None:
        return pd.DataFrame()

    context.resources.delta_io.write_table(
        df, table_name="orbisfn_eod", mode="overwrite", partition_by=["dt"]
    )

    return df


@asset(
    required_resource_keys={"delta_io"},
    description="Orbisfn EOD 股本数据",
    deps=["orbisfn_eod"],  # 添加依赖
)
def orbisfn_shares(
    context: AssetExecutionContext,
    orbisfn_eod: pd.DataFrame,
) -> pd.DataFrame:
    """从 Orbisfn EOD 获取股本数据"""
    # 计算 total_shares 并转为整数
    # 过滤掉空的 symbol
    df = orbisfn_eod.dropna(subset=["SYMBOL"])

    # 计算 total_shares，处理所有异常情况
    shares_df = pd.DataFrame(
        {
            "symbol": df["SYMBOL"],
            "total_shares": (
                (df["MARKET CAP"] / df["LAST PRICE"])
                .replace([np.inf, -np.inf], np.nan)  # 处理无穷大
                .fillna(0)  # 处理 NA 值
                .round(0)  # 四舍五入到整数
                .astype("int64")  # 转换为整数
            ),
            "source": "orbisfn",
            "dt": df["dt"],
        }
    )

    r_delta: S3DeltaResource = context.resources.delta_io
    r_delta.write_table(
        shares_df, table_name="orbisfn_shares", mode="overwrite", partition_by=["dt"]
    )

    return shares_df
