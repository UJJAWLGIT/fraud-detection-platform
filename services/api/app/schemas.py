"""Request/response schemas for the fraud scoring API."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class PaymentRequest(BaseModel):
    txn_id:      str   = Field(...,  examples=["txn_a1b2c3d4"])
    user_id:     str   = Field(...,  examples=["user_9842"])
    amount:      float = Field(...,  ge=0, examples=[1200.50])
    merchant_id: str   = Field(...,  examples=["merch_amazon"])
    device_id:   str   = Field(...,  examples=["dev_ios_7"])
    ip_country:  str   = Field("IN", examples=["IN"])
    lat:         Optional[float] = Field(None, examples=[12.9716])
    lon:         Optional[float] = Field(None, examples=[77.5946])
    ts:          Optional[datetime] = None

    model_config = {"json_schema_extra": {"example": {
        "txn_id": "txn_demo_001",
        "user_id": "user_42",
        "amount": 75000.0,
        "merchant_id": "merch_amazon",
        "device_id": "dev_new_x9f2",
        "ip_country": "RU",
        "lat": 55.75,
        "lon": 37.62,
    }}}


class ScoreResponse(BaseModel):
    txn_id:          str
    decision:        str
    ml_score:        float
    triggered_rules: List[str]
    features:        Dict[str, float]
    latency_ms:      float
    model_version:   str
