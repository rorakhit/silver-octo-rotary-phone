#!/usr/bin/env bash
# =============================================================================
# Demo: Ingest sample data and query all endpoints
#
# Usage:
#   1. Start the server:  python run.py
#   2. In another terminal: bash demo_queries.sh
# =============================================================================

BASE="http://localhost:5001"

echo "============================================"
echo "1. INGEST all three sample files"
echo "============================================"
curl -s -X POST "$BASE/ingest" \
  -F "internal_trades=@sample_data/trades_format1.csv" \
  -F "custodian_trades=@sample_data/trades_format2.csv" \
  -F "bank_positions=@sample_data/positions.yaml" | python3 -m json.tool
echo

echo "============================================"
echo "2. POSITIONS — ACC001 on 2025-01-15"
echo "============================================"
curl -s "$BASE/positions?account=ACC001&date=2025-01-15" | python3 -m json.tool
echo

echo "============================================"
echo "3. COMPLIANCE — concentration check"
echo "============================================"
curl -s "$BASE/compliance/concentration?date=2025-01-15" | python3 -m json.tool
echo

echo "============================================"
echo "4. RECONCILIATION — custodian vs bank"
echo "============================================"
curl -s "$BASE/reconciliation?date=2025-01-15" | python3 -m json.tool
echo

echo "============================================"
echo "5. INGEST — any key name works"
echo "============================================"
curl -s -X POST "$BASE/ingest" \
  -F "whatever=@sample_data/trades_format1.csv" | python3 -m json.tool
echo
