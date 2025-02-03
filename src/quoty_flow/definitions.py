from dagster import (
    get_dagster_logger,
    job,
    op,
    resource,
    InitResourceContext,
    Definitions
)
import os


@resource(config_schema={"hostname": str, "username": str, "password": str, "db_name": str})
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


@job(resource_defs={
    "postgres": postgres_resource.configured({
        "hostname": {"env": "DAGSTER_PG_HOSTNAME"},
        "username": {"env": "DAGSTER_PG_USER"},
        "password": {"env": "DAGSTER_PG_PASSWORD"},
        "db_name": {"env": "DAGSTER_PG_DBNAME"},
    })
})
def debug_job():
    """调试作业"""
    process_data(check_config())


# 定义代码库
defs = Definitions(
    jobs=[debug_job]
)
