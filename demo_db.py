#!/usr/bin/env python3
"""
Demo: Direct database interactions.

Shows SQLAlchemy ORM queries, raw SQL, table contents, deduplication,
and join queries — all against an in-memory SQLite database.

Run:  python demo_db.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import func, text

from app import create_app, db
from app.models import Position, Trade


def section(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def main():
    app = create_app("testing")

    with app.app_context():
        client = app.test_client()

        # ── 1. Show empty tables ─────────────────────────────────────
        section("1. EMPTY DATABASE — tables created via SQLAlchemy")
        print(f"  Trade rows:    {Trade.query.count()}")
        print(f"  Position rows: {Position.query.count()}")

        # Show the schema via raw SQL
        print("\n  Table schemas (from sqlite_master):\n")
        result = db.session.execute(text(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name IN ('trades', 'positions')"
        ))
        for row in result:
            for line in row[0].split("\n"):
                print(f"    {line}")
            print()

        # ── 2. Ingest sample data ────────────────────────────────────
        section("2. INGEST — loading sample data via POST /ingest")
        sample_dir = os.path.join(os.path.dirname(__file__), "sample_data")
        files = {
            "internal_trades": open(os.path.join(sample_dir, "trades_format1.csv"), "rb"),
            "custodian_trades": open(os.path.join(sample_dir, "trades_format2.csv"), "rb"),
            "bank_positions": open(os.path.join(sample_dir, "positions.yaml"), "rb"),
        }
        resp = client.post("/ingest", data=files, content_type="multipart/form-data")
        for f in files.values():
            f.close()

        data = resp.get_json()
        for report in data["reports"]:
            fmt = report.get("format", "unknown")
            filename = os.path.basename(report.get("file", "?"))
            print(f"  {filename:25s}  format={fmt:15s}  "
                  f"inserted={report['rows_inserted']}  "
                  f"dupes={report.get('rows_skipped_duplicate', 0)}")

        # ── 3. Row counts ────────────────────────────────────────────
        section("3. ROW COUNTS — ORM queries")
        print(f"  Trade.query.count()    = {Trade.query.count()}")
        print(f"  Position.query.count() = {Position.query.count()}")

        # ── 4. Sample rows ───────────────────────────────────────────
        section("4. SAMPLE TRADE ROWS — Trade.query.limit(5)")
        trades = Trade.query.limit(5).all()
        header = f"  {'ID':>4}  {'Date':10}  {'Account':8}  {'Ticker':6}  {'Qty':>7}  {'Price':>9}  {'Type':4}  {'Source'}"
        print(header)
        print(f"  {'-' * len(header.strip())}")
        for t in trades:
            price_str = f"{t.price:9.2f}" if t.price else "     None"
            trade_type = t.trade_type or "None"
            print(f"  {t.id:4d}  {t.trade_date.isoformat():10}  {t.account_id:8}  "
                  f"{t.ticker:6}  {t.quantity:7.0f}  {price_str}  {trade_type:4}  {t.source_system}")

        section("5. SAMPLE POSITION ROWS — Position.query.limit(5)")
        positions = Position.query.limit(5).all()
        header = f"  {'ID':>4}  {'Date':10}  {'Account':8}  {'Ticker':6}  {'Shares':>8}  {'Mkt Value':>12}  {'CustRef'}"
        print(header)
        print(f"  {'-' * len(header.strip())}")
        for p in positions:
            print(f"  {p.id:4d}  {p.report_date.isoformat():10}  {p.account_id:8}  "
                  f"{p.ticker:6}  {p.shares:8.0f}  {p.market_value:12.2f}  {p.custodian_ref or 'None'}")

        # ── 6. Aggregation query ─────────────────────────────────────
        section("6. AGGREGATION — trades grouped by source_system")
        rows = (
            db.session.query(
                Trade.source_system,
                func.count(Trade.id).label("trade_count"),
                func.sum(Trade.quantity).label("total_qty"),
            )
            .group_by(Trade.source_system)
            .all()
        )
        print(f"  {'Source System':20}  {'Trades':>8}  {'Total Qty':>12}")
        print(f"  {'-' * 44}")
        for row in rows:
            print(f"  {row.source_system:20}  {row.trade_count:8d}  {row.total_qty:12.0f}")

        # ── 7. Join query — cost basis from trades ───────────────────
        section("7. JOIN — cost basis per position (trades with price data)")
        cost_rows = (
            db.session.query(
                Position.account_id,
                Position.ticker,
                Position.shares.label("bank_shares"),
                Position.market_value.label("bank_mv"),
                func.sum(Trade.quantity * Trade.price).label("cost_basis"),
            )
            .outerjoin(
                Trade,
                (Trade.account_id == Position.account_id)
                & (Trade.ticker == Position.ticker)
                & (Trade.trade_date <= Position.report_date)
                & (Trade.price.isnot(None)),
            )
            .filter(Position.report_date.isnot(None))
            .group_by(Position.account_id, Position.ticker)
            .order_by(Position.account_id, Position.ticker)
            .limit(8)
            .all()
        )
        print(f"  {'Account':8}  {'Ticker':6}  {'Shares':>8}  {'Mkt Value':>12}  {'Cost Basis':>12}")
        print(f"  {'-' * 52}")
        for r in cost_rows:
            cb = f"{r.cost_basis:12.2f}" if r.cost_basis else "        None"
            print(f"  {r.account_id:8}  {r.ticker:6}  {r.bank_shares:8.0f}  "
                  f"{r.bank_mv:12.2f}  {cb}")

        # ── 8. Deduplication demo ────────────────────────────────────
        section("8. DEDUPLICATION — re-ingesting the same file")
        count_before = Trade.query.count()

        files = {
            "trades": open(os.path.join(sample_dir, "trades_format1.csv"), "rb"),
        }
        resp = client.post("/ingest", data=files, content_type="multipart/form-data")
        for f in files.values():
            f.close()

        count_after = Trade.query.count()
        report = resp.get_json()["reports"][0]

        print(f"  Rows before re-ingest:  {count_before}")
        print(f"  Rows after re-ingest:   {count_after}")
        print(f"  New rows inserted:      {report['rows_inserted']}")
        print(f"  Duplicates skipped:     {report['rows_skipped_duplicate']}")
        print(f"  Unique constraint held: {'YES' if count_before == count_after else 'NO'}")

        # ── 9. Raw SQL ───────────────────────────────────────────────
        section("9. RAW SQL — direct query via db.session.execute()")
        result = db.session.execute(text("""
            SELECT account_id,
                   COUNT(DISTINCT ticker) AS unique_tickers,
                   SUM(quantity) AS total_shares
            FROM trades
            GROUP BY account_id
            ORDER BY account_id
        """))
        print(f"  {'Account':8}  {'Tickers':>8}  {'Total Shares':>14}")
        print(f"  {'-' * 34}")
        for row in result:
            print(f"  {row[0]:8}  {row[1]:8d}  {row[2]:14.0f}")

        print(f"\n{'=' * 60}")
        print("  DONE — all queries executed against in-memory SQLite")
        print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
