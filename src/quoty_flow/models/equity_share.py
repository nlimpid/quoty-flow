from pydantic import BaseModel, Field
from datetime import datetime
from decimal import Decimal


class EquityShare(BaseModel):
    """股本数据模型"""

    symbol: str = Field(..., description="股票代码")
    total_shares: int = Field(..., description="总股本")
    source: str = Field(..., description="数据来源")
    dt: str

    class Config:
        json_encoders = {Decimal: str}
