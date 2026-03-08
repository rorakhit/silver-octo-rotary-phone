"""
GET /positions?account=ACC001&date=2026-01-15

Returns positions for an account on a given date, including:
  - shares and market value from the bank-position file
  - cost basis calculated from trades with price data (sum of quantity * price
    on or before the requested date)
"""

from datetime import datetime

from flask import Blueprint, jsonify, request

from app.models import Position, Trade

positions_bp = Blueprint("positions", __name__)


@positions_bp.route("/positions", methods=["GET"])
def get_positions():
    account = request.args.get("account")
    date_str = request.args.get("date")

    if not account or not date_str:
        return jsonify({"error": "Both 'account' and 'date' query parameters are required"}), 400

    try:
        query_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": f"Invalid date format '{date_str}'. Use YYYY-MM-DD"}), 400

    # Bank positions for the requested date
    bank_positions = Position.for_account_and_date(account, query_date)

    if not bank_positions:
        return jsonify({
            "account": account,
            "date": date_str,
            "positions": [],
            "note": "No bank positions found for this account/date",
        }), 200

    # Cost basis per ticker from trades with price data (cumulative up to query_date).
    # The price IS NOT NULL filter is important — custodian trade files typically
    # report market_value but omit per-share price, so they are naturally excluded.
    cost_basis_map = Trade.cost_basis_by_ticker(account, query_date)

    result = []
    total_market_value = 0.0
    for pos in bank_positions:
        mv = pos.market_value
        total_market_value += mv
        result.append({
            "ticker": pos.ticker,
            "shares": pos.shares,
            "market_value": mv,
            "cost_basis": cost_basis_map.get(pos.ticker),
            "custodian_ref": pos.custodian_ref,
        })

    return jsonify({
        "account": account,
        "date": date_str,
        "total_market_value": round(total_market_value, 2),
        "positions": result,
    }), 200
