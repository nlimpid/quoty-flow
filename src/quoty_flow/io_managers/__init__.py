from dagster import IOManager, InputContext, OutputContext
from dagster_deltalake import DeltaLakeIOManager
from ..resources.s3 import S3StorageConfig
import pandas as pd


class S3DeltaIOManager(DeltaLakeIOManager):
    def __init__(self, config: S3StorageConfig):
        super().__init__(
            root_uri=f"s3://{config.bucket}/deltalake",
            storage_options={
                "AWS_ACCESS_KEY_ID": config.access_key,
                "AWS_SECRET_ACCESS_KEY": config.secret_key,
                "AWS_REGION": config.region,
            },
            schema="public",  # default
        )

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # 自定义输出处理逻辑
        super().handle_output(context, obj)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        # 自定义输入加载逻辑
        return super().load_input(context)
