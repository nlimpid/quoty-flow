from dagster import ResourceDependency, ConfigurableResource
from ..resources.s3 import S3StorageConfig
import pandas as pd
from typing import Optional, List


class S3DeltaResource(ConfigurableResource):
    credentials: ResourceDependency[S3StorageConfig]

    @property
    def storage_options(self) -> dict:
        """动态生成存储配置"""
        base_options = {
            "AWS_ACCESS_KEY_ID": self.credentials.access_key,
            "AWS_SECRET_ACCESS_KEY": self.credentials.secret_key,
            "AWS_REGION": self.credentials.region,
        }

        if (
            self.credentials.endpoint_url
            and "amazonaws.com" not in self.credentials.endpoint_url
        ):
            base_options.update(
                {
                    "AWS_ENDPOINT_URL": self.credentials.endpoint_url,
                    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
                    "AWS_ALLOW_HTTP": str(not self.credentials.use_ssl).lower(),
                }
            )

        return base_options

    def get_table_path(self, table_name: str) -> str:
        """获取完整的表路径"""
        return f"s3://{self.credentials.bucket}/deltalake/{table_name}"

    def write_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
    ):
        """写入 Delta 表
        Args:
            df: 要写入的数据框
            table_name: 表名
            mode: 写入模式，可选 'append' 或 'overwrite'
        """
        import deltalake

        table_path = self.get_table_path(table_name)

        # 检查表是否存在
        try:
            deltalake.DeltaTable(table_path)
            # 表已存在，直接写入，无需指定 partition_by
            deltalake.write_deltalake(
                table_path,
                df,
                mode=mode,
                storage_options=self.storage_options,
            )
        except Exception:
            # 表不存在，首次创建时指定分区
            deltalake.write_deltalake(
                table_path,
                df,
                mode=mode,
                partition_by=partition_by,
                storage_options=self.storage_options,
            )

    def read_table(self, table_name: str) -> pd.DataFrame:
        """读取 Delta 表
        Args:
            table_name: 表名
        Returns:
            pd.DataFrame: 读取的数据框
        """
        import deltalake

        return deltalake.DeltaTable(
            self.get_table_path(table_name), storage_options=self.storage_options
        ).to_pandas()
