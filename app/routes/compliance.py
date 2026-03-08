"""
GET /compliance/concentration?date=2026-01-15

Checks each account for concentration violations: any single equity whose
market value exceeds 20% of the total account market value (from bank positions).

Returns only accounts/tickers that breach the threshold.
"""

from datetime import datetime

from flask import Blueprint, jsonify, request

from app.models import Position

compliance_bp = Blueprint("compliance", __name__)

CONCENTRATION_THRESHOLD = 0.20  # 20%


@compliance_bp.route("/compliance/concentration", methods=["GET"])
def concentration():
    date_str = request.args.get("date")

    if not date_str:
        return jsonify({"error": "'date' query parameter is required"}), 400

    try:
        query_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": f"Invalid date format '{date_str}'. Use YYYY-MM-DD"}), 400

    # Two-pass approach:
    # 1st query — aggregate total market value per account (denominator)
    # 2nd query — iterate individual positions to check each against the threshold
    total_mv_map = Position.account_totals(query_date)

    if not total_mv_map:
        return jsonify({
            "date": date_str,
            "violations": [],
            "note": "No positions found for this date",
        }), 200

    # Check each position's weight: position_mv / account_total_mv
    positions = Position.for_date(query_date)

    violations = []
    for pos in positions:
        account_total = total_mv_map.get(pos.account_id, 0)
        if account_total <= 0:
            continue
        concentration = pos.market_value / account_total
        if concentration > CONCENTRATION_THRESHOLD:
            violations.append({
                "account_id": pos.account_id,
                "ticker": pos.ticker,
                "position_market_value": round(pos.market_value, 2),
                "account_total_market_value": round(account_total, 2),
                "concentration_pct": round(concentration * 100, 2),
                "threshold_pct": CONCENTRATION_THRESHOLD * 100,
                "excess_pct": round((concentration - CONCENTRATION_THRESHOLD) * 100, 2),
            })

    violations.sort(key=lambda v: (-v["concentration_pct"], v["account_id"]))

    return jsonify({
        "date": date_str,
        "threshold_pct": CONCENTRATION_THRESHOLD * 100,
        "total_violations": len(violations),
        "violations": violations,
    }), 200
