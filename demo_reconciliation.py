#!/usr/bin/env python3
"""
Standalone demo: ingest sample data and validate reconciliation logic.

Run:  python demo_reconciliation.py

No server needed — uses the Flask test client directly.
"""

import json
import os
import sys

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(__file__))

from app import create_app, db


def main():
    app = create_app("testing")

    with app.app_context():
        client = app.test_client()

        # ── 1. Ingest ───────────────────────────────────────────────
        sample_dir = os.path.join(os.path.dirname(__file__), "sample_data")
        files = {
            "internal_trades": open(os.path.join(sample_dir, "trades_format1.csv"), "rb"),
            "custodian_trades": open(os.path.join(sample_dir, "trades_format2.csv"), "rb"),
            "bank_positions": open(os.path.join(sample_dir, "positions.yaml"), "rb"),
        }
        resp = client.post("/ingest", data=files, content_type="multipart/form-data")
        for f in files.values():
            f.close()

        ingest_data = resp.get_json()
        print("=" * 60)
        print("INGESTION REPORT")
        print("=" * 60)
        for r in ingest_data["reports"]:
            print(f"  {r['format']:12s}  rows_inserted={r['rows_inserted']}  "
                  f"duplicates={r.get('rows_skipped_duplicate', 0)}  "
                  f"issues={len(r.get('quality_issues', []))}")
        print()

        # ── 2. Positions ────────────────────────────────────────────
        print("=" * 60)
        print("POSITIONS — ACC001 on 2025-01-15")
        print("=" * 60)
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        pretty(resp.get_json())

        # ── 3. Compliance ───────────────────────────────────────────
        print("=" * 60)
        print("COMPLIANCE — concentration violations on 2025-01-15")
        print("=" * 60)
        resp = client.get("/compliance/concentration?date=2025-01-15")
        data = resp.get_json()
        if data["violations"]:
            for v in data["violations"]:
                print(f"  {v['account_id']} / {v['ticker']}: "
                      f"{v['concentration_pct']:.1f}% of "
                      f"${v['account_total_market_value']:,.2f} "
                      f"(excess: {v['excess_pct']:.1f}%)")
        else:
            print("  No violations found.")
        print()

        # ── 4. Reconciliation ───────────────────────────────────────
        print("=" * 60)
        print("RECONCILIATION — custodian vs bank on 2025-01-15")
        print("=" * 60)
        resp = client.get("/reconciliation?date=2025-01-15")
        data = resp.get_json()

        summary = data["summary"]
        print(f"\n  Summary: matched={summary['matched']}  "
              f"shares_mismatch={summary['shares_mismatch']}  "
              f"missing_in_bank={summary['missing_in_bank']}  "
              f"missing_in_trades={summary['missing_in_trades']}")
        print()

        for d in data["discrepancies"]:
            dtype = d["type"]
            if dtype == "shares_mismatch":
                print(f"  MISMATCH  {d['account_id']}/{d['ticker']}: "
                      f"custodian={d['custodian_shares']} vs bank={d['bank_shares']} "
                      f"(diff={d['share_difference']})")
            elif dtype == "missing_in_bank":
                print(f"  MISSING IN BANK   {d['account_id']}/{d['ticker']}: "
                      f"custodian has {d['custodian_shares']} shares, bank has none")
            else:
                print(f"  MISSING IN TRADES {d['account_id']}/{d['ticker']}: "
                      f"bank has {d['bank_shares']} shares, custodian has none")
        print()

        print("=" * 60)
        print("SAMPLE DATA DISCREPANCIES EXPLAINED")
        print("=" * 60)
        print("""
  The sample data has intentional discrepancies between custodian
  trades and bank (positions.yaml) records:

  ACC001/GOOGL: Custodian says 100 shares, bank says 75 → shares_mismatch
  ACC002/GOOGL: Custodian has 75 shares, bank has no record → missing_in_bank
  ACC002/TSLA:  Bank has 80 shares, custodian has no record → missing_in_trades
  ACC003:       Custodian has TSLA+NVDA; bank has MSFT+GOOGL+AAPL → multiple mismatches
  ACC004:       Custodian has AAPL+MSFT; bank has no ACC004 positions → missing_in_bank

  These mismatches demonstrate all three discrepancy types the
  reconciliation endpoint detects.
""")


def pretty(data):
    print(json.dumps(data, indent=2))
    print()


if __name__ == "__main__":
    main()
