import pandas as pd
from dagster import RunConfig, asset, Output, EnvVar, DailyPartitionsDefinition
from dagster import AssetExecutionContext  # 新增导入
from dagster import (  # type: ignore
    get_dagster_logger,
    job,
    load_assets_from_modules,
    op,
    resource,
    InitResourceContext,
    Definitions,
)
import os

from src.quoty_flow.resources.hkex import HKEXScraperResource
from src.quoty_flow.assets.hkex import (
    check_bond_data_quality,
    raw_bond_data,
    staged_bond_data,
    published_bond_data,
)
from src.quoty_flow.assets import hkex, equity_share

from src.quoty_flow.resources.equity_share import OrbisfnResource, YahooFinanceResource


from src.quoty_flow.io_managers.deltalake_io import S3DeltaResource
from src.quoty_flow.resources.s3 import S3StorageConfig

# 定义日期分区
daily_partitions = DailyPartitionsDefinition(
    start_date="2024-01-01",  # 根据实际需求调整起始日期
)


@resource(
    config_schema={"hostname": str, "username": str, "password": str, "db_name": str}
)
def postgres_resource(context: InitResourceContext):
    """Postgres连接资源"""
    return {
        "hostname": os.getenv("DAGSTER_PG_HOSTNAME"),
        "username": os.getenv("DAGSTER_PG_USER"),
        "password": os.getenv("DAGSTER_PG_PASSWORD"),
        "db_name": os.getenv("DAGSTER_PG_DBNAME"),
    }


@op(required_resource_keys={"postgres"})
def check_config(context):
    """检查配置是否正确加载"""
    logger = get_dagster_logger()
    postgres_config = context.resources.postgres

    # 打印配置信息（实际生产环境不要这样做）
    logger.info(f"DB Host: {postgres_config['hostname']}")
    logger.info(f"DB User: {postgres_config['username']}")
    logger.info(f"DB Name: {postgres_config['db_name']}")
    return "配置检查完成"


@op(required_resource_keys={"postgres"})
def process_data(context, config_check_result):
    """处理数据的示例操作"""
    logger = get_dagster_logger()
    logger.info(f"前序操作结果: {config_check_result}")
    return "数据处理完成"


@job(
    resource_defs={
        "postgres": postgres_resource.configured(
            {
                "hostname": {"env": "DAGSTER_PG_HOSTNAME"},
                "username": {"env": "DAGSTER_PG_USER"},
                "password": {"env": "DAGSTER_PG_PASSWORD"},
                "db_name": {"env": "DAGSTER_PG_DBNAME"},
            }
        )
    }
)
def debug_job():
    """调试作业"""
    process_data(check_config())


@asset(required_resource_keys={"delta_io"})
def iris_dataset(context: AssetExecutionContext) -> pd.DataFrame:
    df = pd.read_csv(
        "https://docs.dagster.io/assets/iris.csv",
        names=[
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
            "species",
        ],
    )

    # 显式调用 delta resource 的写入方法

    context.resources.delta_io.write_table(
        df, table_name="iris_dataset", mode="overwrite", partition_by=["species"]
    )

    return df  # 保持返回 DataFrame 供下游使用（可选）


# 从环境变量创建配置
s3_config = S3StorageConfig(
    access_key=EnvVar("S3_ACCESS_KEY_ID"),
    secret_key=EnvVar("S3_SECRET_KEY"),
    bucket=EnvVar("S3_BUCKET"),
    region=EnvVar("S3_REGION"),
    endpoint_url=EnvVar("S3_ENDPOINT_URL"),
    use_ssl=EnvVar("S3_USE_SSL") == "true",
)

assets_modules = [
    *load_assets_from_modules([hkex]),
    *load_assets_from_modules([equity_share]),
]

# 定义代码库
defs = Definitions(
    # assets=[*load_assets_from_modules([assets])],
    assets=[*assets_modules, iris_dataset],
    asset_checks=[check_bond_data_quality],
    jobs=[debug_job],
    resources={
        "s3": s3_config,
        "delta_io": S3DeltaResource(credentials=s3_config),
        "hkex_scraper": HKEXScraperResource(),
        # # 添加股本数据相关资源
        # "share_capital_source1": ShareCapitalSource1Resource(),
        # "share_capital_source2": ShareCapitalSource2Resource(),
        # "share_capital_source3": ShareCapitalSource3Resource(),
        # "share_capital_source4": ShareCapitalSource4Resource(),
        # "share_capital_diff": ShareCapitalDiffResource(),
        # # 添加股本数据源
        "yahoo_finance": YahooFinanceResource(batch_size=50, max_workers=10),
        # "nasdaq_screener": NasdaqScreenerResource(),
        "orbisfn": OrbisfnResource(),
    },
)
