"""Ensemble decision: hard rules + ML score → final verdict."""
from __future__ import annotations

from typing import List, Tuple


def decide(features: dict, ml_score: float) -> Tuple[str, List[str]]:
    """
    Combine rule engine output with ML score.

    Rules are checked separately by the caller (rules.evaluate).
    This function takes the severity outcome and blends it with ML confidence.

    Returns (decision, triggered_rules).
    """
    from .rules import evaluate
    triggered, rule_severity = evaluate(features)

    # Hard rule says BLOCK — model cannot override
    if rule_severity == "BLOCK":
        return "BLOCK", triggered

    # Model is very confident → BLOCK regardless of rules
    if ml_score >= 0.85:
        return "BLOCK", triggered

    # Rules flagged + model agrees → escalate to BLOCK
    if rule_severity == "REVIEW" and ml_score >= 0.60:
        return "BLOCK", triggered

    # Rules flagged but model is uncertain → keep REVIEW
    if rule_severity == "REVIEW":
        return "REVIEW", triggered

    # Model sees medium risk → manual review
    if ml_score >= 0.45:
        return "REVIEW", triggered

    return "APPROVE", triggered
