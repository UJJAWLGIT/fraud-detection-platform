"""Unit tests for the rule engine."""
from app.rules import evaluate


def _f(**kwargs):
    """Build a features dict with safe defaults."""
    defaults = dict(
        amount=500.0, txn_count_5m=1, txn_count_1m=0, txn_count_1h=2,
        amount_ratio=1.0, avg_30d=500.0, new_device=0.0, is_new_user=0.0,
        geo_distance_km=5.0, is_foreign_ip=0.0, merch_risk_30d=0.02,
    )
    return {**defaults, **kwargs}


def test_clean_transaction_passes():
    _, sev = evaluate(_f())
    assert sev == "PASS"


def test_extreme_amount_blocks():
    triggered, sev = evaluate(_f(amount=250_000))
    assert sev == "BLOCK"
    assert "EXTREME_AMOUNT" in triggered


def test_high_velocity_blocks():
    triggered, sev = evaluate(_f(txn_count_5m=12))
    assert sev == "BLOCK"
    assert "EXTREME_VELOCITY_5M" in triggered


def test_extreme_geo_blocks():
    triggered, sev = evaluate(_f(geo_distance_km=4000))
    assert sev == "BLOCK"
    assert "EXTREME_GEO_JUMP" in triggered


def test_high_amount_review():
    triggered, sev = evaluate(_f(amount=60_000))
    assert sev == "REVIEW"
    assert "HIGH_AMOUNT" in triggered


def test_foreign_ip_review():
    triggered, sev = evaluate(_f(is_foreign_ip=1.0))
    assert sev == "REVIEW"
    assert "FOREIGN_IP" in triggered


def test_new_device_review():
    triggered, sev = evaluate(_f(new_device=1.0))
    assert sev == "REVIEW"
    assert "NEW_DEVICE" in triggered
