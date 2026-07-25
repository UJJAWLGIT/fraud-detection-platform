"""Unit tests for ensemble decision logic."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.decision import decide


def _clean_features():
    return dict(
        amount=500.0, txn_count_5m=1, txn_count_1m=0, txn_count_1h=2,
        amount_ratio=1.0, avg_30d=500.0, new_device=0.0, is_new_user=0.0,
        geo_distance_km=5.0, is_foreign_ip=0.0, merch_risk_30d=0.02,
    )


def test_clean_approve():
    d, _ = decide(_clean_features(), 0.10)
    assert d == "APPROVE"


def test_high_ml_blocks():
    d, _ = decide(_clean_features(), 0.90)
    assert d == "BLOCK"


def test_medium_ml_review():
    d, _ = decide(_clean_features(), 0.55)
    assert d == "REVIEW"


def test_block_rule_wins_over_low_ml():
    f = {**_clean_features(), "amount": 250_000}
    d, triggered = decide(f, 0.05)
    assert d == "BLOCK"
    assert "EXTREME_AMOUNT" in triggered


def test_review_plus_high_ml_escalates():
    f = {**_clean_features(), "amount": 60_000}  # HIGH_AMOUNT → REVIEW
    d, _ = decide(f, 0.70)  # ml also high → escalate to BLOCK
    assert d == "BLOCK"
