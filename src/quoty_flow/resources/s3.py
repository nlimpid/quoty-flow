from dagster import ConfigurableResource


class S3StorageConfig(ConfigurableResource):
    # S3 认证信息
    access_key: str
    secret_key: str

    # S3 基础配置
    bucket: str
    region: str

    # S3 endpoint 配置
    endpoint_url: str

    # 可选：是否使用 SSL
    use_ssl: bool
