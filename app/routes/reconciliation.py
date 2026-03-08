"""
GET /reconciliation?date=2026-01-15

Compares custodian-sourced trade positions against bank positions for a given date.
Custodian trades are identified by source_system != 'internal'.

Reports three categories of discrepancy:

  - shares_mismatch   : Both sources have the position but share counts differ
  - missing_in_bank   : Custodian has a position; bank does not
  - missing_in_trades : Bank has a position; custodian does not
"""

from datetime import datetime

from flask import Blueprint, jsonify, request

from app.models import Position, Trade

reconciliation_bp = Blueprint("reconciliation", __name__)


@reconciliation_bp.route("/reconciliation", methods=["GET"])
def reconciliation():
    date_str = request.args.get("date")

    if not date_str:
        return jsonify({"error": "'date' query parameter is required"}), 400

    try:
        query_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": f"Invalid date format '{date_str}'. Use YYYY-MM-DD"}), 400

    # Build lookup dicts keyed by (account_id, ticker) for both sides
    custodian_map = Trade.custodian_positions(query_date)
    bank_map = Position.as_lookup(query_date)

    # Union of all keys — ensures we catch positions that exist in only one source
    all_keys = set(custodian_map) | set(bank_map)

    discrepancies = []
    matched = 0

    for key in sorted(all_keys):
        account_id, ticker = key
        cust = custodian_map.get(key)
        bank = bank_map.get(key)

        if cust and bank:
            # Tolerance-based comparison to avoid false positives from
            # floating-point rounding (1e-4 for shares, 0.01 for dollars)
            share_diff = round(cust["shares"] - bank["shares"], 6)
            mv_diff = round((cust["market_value"] or 0) - bank["market_value"], 2)
            if abs(share_diff) > 1e-4 or abs(mv_diff) > 0.01:
                discrepancies.append({
                    "type": "shares_mismatch",
                    "account_id": account_id,
                    "ticker": ticker,
                    "custodian_shares": cust["shares"],
                    "bank_shares": bank["shares"],
                    "share_difference": share_diff,
                    "custodian_market_value": cust["market_value"],
                    "bank_market_value": bank["market_value"],
                    "market_value_difference": mv_diff,
                })
            else:
                matched += 1

        elif cust and not bank:
            discrepancies.append({
                "type": "missing_in_bank",
                "account_id": account_id,
                "ticker": ticker,
                "custodian_shares": cust["shares"],
                "custodian_market_value": cust["market_value"],
            })

        else:  # bank and not cust
            discrepancies.append({
                "type": "missing_in_trades",
                "account_id": account_id,
                "ticker": ticker,
                "bank_shares": bank["shares"],
                "bank_market_value": bank["market_value"],
            })

    summary = {
        "matched": matched,
        "shares_mismatch": sum(1 for d in discrepancies if d["type"] == "shares_mismatch"),
        "missing_in_bank": sum(1 for d in discrepancies if d["type"] == "missing_in_bank"),
        "missing_in_trades": sum(1 for d in discrepancies if d["type"] == "missing_in_trades"),
    }

    return jsonify({
        "date": date_str,
        "summary": summary,
        "discrepancies": discrepancies,
    }), 200
