from pydantic import BaseModel, field_validator, Field
from datetime import datetime
from typing import Optional
import re


class HKEXBondRate(BaseModel):
    symbol: str = Field(..., description="Bond symbol/code")
    payment_date: datetime = Field(..., description="Interest payment date")
    determination_date: datetime = Field(...,
                                         description="Interest determination date")
    interest_rate: float = Field(..., description="Interest rate")
    bond_type: str = Field(..., description="iBond or GreenBond")

    @field_validator('symbol')
    @classmethod
    def validate_symbol(cls, v):
        if not re.match(r'^\d{4}$', v):
            raise ValueError("Symbol must be 4 digits")
        return v

    @field_validator('interest_rate')
    @classmethod
    def validate_rate(cls, v):
        if v < 0 or v > 100:
            raise ValueError("Interest rate must be between 0 and 100")
        return v
