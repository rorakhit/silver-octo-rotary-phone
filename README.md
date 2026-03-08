# Portfolio Data Clearinghouse

A portfolio data reconciliation system built with Flask + SQLAlchemy. Ingests trade and position data from multiple sources/formats, stores it in a unified relational database, and exposes endpoints for portfolio metrics, compliance checks, and reconciliation.

## Quick Start

```bash
# 1. Clone and install (Python 3.10+)
git clone <repo-url> && cd silver-octo-rotary-phone
pip install -r requirements.txt

# 2. Run tests (69 tests, ~0.3s)
python -m pytest tests/ -v

# 3. Run demos (no server needed — uses in-memory SQLite)
python demo_db.py                # DB schema, ORM queries, raw SQL, joins, dedup
python demo_reconciliation.py    # End-to-end: ingest, positions, compliance, reconciliation

# 4. Start the server and hit the API
python run.py                    # http://localhost:5000, persists to portfolio.db
bash demo_queries.sh             # Ingests sample data + queries all endpoints via curl
```

No external services required. SQLite is used for storage (file-backed in server mode, in-memory for tests and demos). Ticker validation calls SEC EDGAR and OpenFIGI but fails open if unreachable.

## Endpoints

### `POST /ingest`

Ingest one or more data files. Accepts `multipart/form-data` — upload files under **any key name**. Every file is auto-detected:

1. **Format detection** — YAML/JSON structured data vs delimited tabular data
2. **Delimiter sniffing** — comma, pipe, tab, semicolon
3. **Column mapping** — headers are matched to canonical fields using regex patterns, so non-standard names work automatically (`Exec_Date`, `Transaction_Date`, `trade_dt` all resolve to trade date)
4. **Classification** — file is routed to trades or positions based on which fields are present

Returns a data quality report per file: rows inserted, duplicates skipped, column mapping used, and any validation issues.

```bash
curl -X POST http://localhost:5000/ingest \
  -F "trades=@sample_data/trades_format1.csv" \
  -F "custodian=@sample_data/trades_format2.csv" \
  -F "positions=@sample_data/positions.yaml"
```

### `GET /positions?account=ACC001&date=2025-01-15`

Returns bank positions for an account on a given date, with market value and cost basis (computed from trades that have price data).

### `GET /compliance/concentration?date=2025-01-15`

Identifies accounts where any single equity exceeds 20% of total account market value.

Each violation includes: account, ticker, concentration %, account total market value, and excess above threshold.

### `GET /reconciliation?date=2025-01-15`

Compares custodian-sourced trades (`source_system != "internal"`) against bank positions. Returns three discrepancy types:

- `shares_mismatch` — both sources have the position but share counts differ
- `missing_in_bank` — custodian has it, bank does not
- `missing_in_trades` — bank has it, custodian does not

## Supported Input Formats

The system is **format-agnostic** — there are no hardcoded format handlers. Any tabular or structured file is accepted and auto-detected.

### Tabular files (trades or positions)

Any delimiter (comma, pipe, tab, semicolon) is auto-detected. Column headers are matched via regex:

```
TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate
2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17
```

```
REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM
20250115|ACC001|AAPL|100|18550.00|CUSTODIAN_A
```

Files with a `SOURCE_SYSTEM`, `Custodian`, `Broker`, or similar column will have that value captured; files without such a column default to `source_system = "internal"`. This distinction drives reconciliation (custodian vs internal trades).

### Structured files (YAML/JSON positions)

```yaml
report_date: "20250115"
positions:
  - account_id: "ACC001"
    ticker: "AAPL"
    shares: 100
    market_value: 18550.00
    custodian_ref: "CUST_A_12345"
```

Top-level keys like `positions`, `holdings`, `portfolio`, and date keys like `report_date`, `as_of_date`, `snapshot_date` are all resolved via regex.

### Dates

Accepted in multiple formats: `YYYY-MM-DD`, `YYYYMMDD`, `MM/DD/YYYY`, and others.

## Ticker Validation

Ingested tickers are validated against a three-tier lookup:

1. **SEC EDGAR** — ~10K US-listed tickers, cached locally and refreshed daily
2. **Local DB** — tickers already ingested in prior runs are implicitly trusted (no external call needed)
3. **OpenFIGI** — Bloomberg's open API for international tickers (e.g. `VOW3.DE`, `SIE.DE`), results cached permanently per-ticker

All tiers fail-open: if SEC and OpenFIGI are unreachable, ingestion proceeds without blocking. Unrecognized tickers produce a warning in `quality_issues` but are still ingested.

## Data Quality Checks

Every ingestion call returns a per-file quality report that includes:
- `rows_inserted` / `rows_skipped_duplicate` — deduplication is enforced via unique constraints
- `quality_issues` — array of specific problems found:
  - Missing required columns
  - Unparseable dates
  - Unmapped/unknown columns
  - Missing date fields in structured files
  - Unrecognized tickers (not found in SEC, DB, or OpenFIGI)

## Demo Scripts

All demos run against an in-memory SQLite database — no server or external dependencies needed.

### `python demo_db.py` — Database interactions

Shows direct SQLAlchemy ORM queries, raw SQL, and DB behavior:

1. Empty database — prints the `CREATE TABLE` schema from `sqlite_master`
2. Ingest — loads all 3 sample files via `POST /ingest`
3. Row counts — `Trade.query.count()`, `Position.query.count()`
4. Sample rows — formatted output from `Trade.query.limit(5)`
5. Aggregation — `GROUP BY source_system` showing internal vs custodian trades
6. Join query — outer join of positions to trades for cost basis
7. Deduplication — re-ingests the same file, shows 0 new rows inserted
8. Raw SQL — `db.session.execute(text(...))` with a hand-written query

### `python demo_reconciliation.py` — End-to-end reconciliation

Ingests sample data, queries every endpoint, and explains each reconciliation discrepancy:

```
RECONCILIATION — custodian vs bank on 2025-01-15

  Summary: matched=4  shares_mismatch=1  missing_in_bank=5  missing_in_trades=4

  MISMATCH  ACC001/GOOGL: custodian=100.0 vs bank=75.0 (diff=25.0)
  MISSING IN BANK   ACC002/GOOGL: custodian has 75.0 shares, bank has none
  MISSING IN TRADES ACC002/TSLA: bank has 80.0 shares, custodian has none
  ...
```

### `bash demo_queries.sh` — curl-based (requires running server)

```bash
python run.py &          # start server
bash demo_queries.sh     # ingest + query all endpoints
```

## Tests

```bash
python -m pytest tests/ -v
```

Tests covering:
- Format-agnostic ingestion (arbitrary headers, multiple delimiters)
- Column mapping via regex
- Trade vs position classification heuristic
- Deduplication
- Data quality reporting (missing columns, unmapped fields, bad dates)
- Three-tier ticker validation (SEC cache, DB fallback, OpenFIGI)
- All four query endpoints (positions, compliance, reconciliation)

## Project Structure

```
├── app/
│   ├── __init__.py               # App factory
│   ├── models.py                 # SQLAlchemy models (Trade, Position)
│   ├── routes/
│   │   ├── ingest.py             # POST /ingest
│   │   ├── positions.py          # GET /positions
│   │   ├── compliance.py         # GET /compliance/concentration
│   │   └── reconciliation.py     # GET /reconciliation
│   └── services/
│       ├── ingestion.py          # Parsing + auto-detect logic
│       └── ticker_registry.py    # Three-tier ticker validation
├── tests/
│   ├── conftest.py               # Fixtures (app, db, client, seed_data)
│   ├── test_ingest.py            # Core ingestion tests
│   ├── test_auto_ingest.py       # Column mapping + delimiter detection tests
│   ├── test_auto_positions.py    # Position ingestion tests
│   ├── test_positions.py         # GET /positions tests
│   ├── test_compliance.py        # GET /compliance/concentration tests
│   ├── test_reconciliation.py    # GET /reconciliation tests
│   └── test_ticker_registry.py   # Ticker validation tests
├── sample_data/
│   ├── trades_format1.csv
│   ├── trades_format2.csv
│   └── positions.yaml
├── demo_db.py                    # DB interaction demo (ORM, raw SQL, joins)
├── demo_reconciliation.py        # End-to-end reconciliation demo
├── demo_queries.sh               # curl-based demo queries
├── config.py
├── run.py
└── requirements.txt
```
