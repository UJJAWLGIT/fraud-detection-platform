"""
Hard rule engine — runs before the ML model.

Rules fire independently. Decision takes the worst severity.
Block rules are absolute — no ML score can override them.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class Rule:
    name:     str
    severity: str   # BLOCK | REVIEW
    reason:   str   # human-readable, shown in REVIEW queue


RULES: List[Rule] = [
    # ── BLOCK — no business justification for amounts this large ─────────
    Rule("EXTREME_AMOUNT",        "BLOCK",  "Amount > ₹2,00,000 absolute threshold"),
    Rule("EXTREME_VELOCITY_5M",   "BLOCK",  "10+ transactions in 5 minutes"),
    Rule("EXTREME_AMOUNT_RATIO",  "BLOCK",  "Amount is 20x+ the user's 30d average"),
    Rule("EXTREME_GEO_JUMP",      "BLOCK",  "Transaction > 3,000 km from home"),

    # ── REVIEW — suspicious but may be legitimate ─────────────────────────
    Rule("HIGH_AMOUNT",           "REVIEW", "Amount > ₹50,000"),
    Rule("HIGH_VELOCITY_5M",      "REVIEW", "5+ transactions in 5 minutes"),
    Rule("HIGH_AMOUNT_RATIO",     "REVIEW", "Amount 10x+ the 30d average"),
    Rule("NEW_DEVICE",            "REVIEW", "First-seen device this month"),
    Rule("NEW_USER",              "REVIEW", "No transaction history in last 30 days"),
    Rule("FOREIGN_IP",            "REVIEW", "IP originates outside India"),
    Rule("GEO_ANOMALY",           "REVIEW", "Transaction > 500 km from home location"),
    Rule("HIGH_MERCHANT_RISK",    "REVIEW", "Merchant fraud rate > 5% last 30 days"),
]


def evaluate(features: Dict[str, float]) -> Tuple[List[str], str]:
    """
    Returns (triggered_rule_names, worst_severity).
    worst_severity: "BLOCK" | "REVIEW" | "PASS"
    """
    triggered: List[str] = []

    amount      = features.get("amount", 0)
    vel5m       = features.get("txn_count_5m", 0)
    ratio       = features.get("amount_ratio", 1)
    geo_km      = features.get("geo_distance_km", 0)
    new_device  = features.get("new_device", 0)
    new_user    = features.get("is_new_user", 0)
    foreign_ip  = features.get("is_foreign_ip", 0)
    merch_risk  = features.get("merch_risk_30d", 0)

    # BLOCK
    if amount     > 200_000:  triggered.append("EXTREME_AMOUNT")
    if vel5m      >= 10:      triggered.append("EXTREME_VELOCITY_5M")
    if ratio      > 20.0:     triggered.append("EXTREME_AMOUNT_RATIO")
    if geo_km     > 3_000:    triggered.append("EXTREME_GEO_JUMP")

    # REVIEW
    if amount     > 50_000:   triggered.append("HIGH_AMOUNT")
    if vel5m      >= 5:       triggered.append("HIGH_VELOCITY_5M")
    if ratio      > 10.0:     triggered.append("HIGH_AMOUNT_RATIO")
    if new_device == 1.0:     triggered.append("NEW_DEVICE")
    if new_user   == 1.0:     triggered.append("NEW_USER")
    if foreign_ip == 1.0:     triggered.append("FOREIGN_IP")
    if geo_km     > 500:      triggered.append("GEO_ANOMALY")
    if merch_risk > 0.05:     triggered.append("HIGH_MERCHANT_RISK")

    rule_map = {r.name: r for r in RULES}
    severities = {rule_map[r].severity for r in triggered if r in rule_map}

    if "BLOCK"  in severities: return triggered, "BLOCK"
    if "REVIEW" in severities: return triggered, "REVIEW"
    return triggered, "PASS"
