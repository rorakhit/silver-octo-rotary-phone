"""
Data ingestion service.

All files are auto-detected — there are no format-specific handlers.

Detection logic:
  1. Tries YAML/JSON – if the parsed object contains a positions/holdings key,
     it is routed to the bank-position ingester.
  2. Otherwise treats the content as delimited text, sniffs the delimiter, and
     fuzzy-matches column headers against known patterns to map them onto our
     canonical Trade or Position schema.
"""

import csv
import io
import json
import re
from datetime import date, datetime

import yaml

from app import db
from app.models import Position, Trade
from app.services.ticker_registry import validate_tickers


# ---------------------------------------------------------------------------
# Column matching – regex patterns for canonical trade fields
#
# Each pattern is tried against the normalised header (lowercased, whitespace/
# hyphens replaced with underscores).  Patterns are evaluated in order; the
# FIRST match wins, so more specific patterns come before general ones.
# ---------------------------------------------------------------------------

COLUMN_PATTERNS: list[tuple[str, re.Pattern]] = [
    # dates
    ("trade_date",      re.compile(r"(trade|report|transaction|trans|exec(ution)?).*(date|dt)$|^date$")),
    ("settlement_date", re.compile(r"(settle(ment)?|value).*(date|dt)$")),
    # identifiers
    ("account_id",      re.compile(r"(account|acct|portfolio).*(id|number|no)?$|^(account|acct|portfolio)$")),
    ("ticker",          re.compile(r"(ticker|symbol|security|sec|instrument).*(ticker|id)?$|^(ticker|symbol)$")),
    # numeric
    ("quantity",        re.compile(r"(quantity|qty|shares|units|volume|size|lots)$|^(trade|exec)_(qty|quantity)$")),
    ("price",           re.compile(r"(price|px)$|^(trade|exec(ution)?|unit|avg)_(price|px)$")),
    ("market_value",    re.compile(r"(market|mkt|total|net).*(value|val|mv)$|^(notional|mv|market_value)$")),
    # categorical
    ("trade_type",      re.compile(r"(trade.?type|side|direction|action|buy.?sell|bs|trans(action)?.?type|order.?type)$")),
    ("source_system",   re.compile(r"(source|custodian|broker|provider|feed).*(system|id|name)?$|^(source|custodian|broker)$")),
]

# Minimum fields we must resolve to create a Trade row
_REQUIRED_TRADE = {"trade_date", "account_id", "ticker", "quantity"}

# Fields that are trade-specific (used to distinguish trades from positions)
_TRADE_ONLY_FIELDS = {"price", "trade_type", "settlement_date"}


# ---------------------------------------------------------------------------
# Position column matching – regex patterns for bank/broker positions
# ---------------------------------------------------------------------------

POSITION_COLUMN_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("report_date",   re.compile(r"(report|snapshot|as_of|effective|valuation).*(date|dt)$|^date$")),
    ("account_id",    re.compile(r"(account|acct|portfolio).*(id|number|no)?$|^(account|acct|portfolio)$")),
    ("ticker",        re.compile(r"(ticker|symbol|security|sec|instrument).*(ticker|id)?$|^(ticker|symbol)$")),
    ("shares",        re.compile(r"(shares|quantity|qty|units|holdings|lots|position)$")),
    ("market_value",  re.compile(r"(market|mkt|total|net|current).*(value|val|mv)$|^(notional|mv|market_value|value)$")),
    ("custodian_ref", re.compile(r"(custodian|cust|broker).*(ref(erence)?|id|code)$|^(custodian_ref|cust_ref)$")),
]

# Minimum fields for a position row
_REQUIRED_POSITION = {"report_date", "account_id", "ticker", "shares", "market_value"}

# Position-specific fields (helps distinguish from trades)
_POSITION_ONLY_FIELDS = {"custodian_ref"}

# Patterns for identifying the top-level positions list key in YAML/JSON
_POSITIONS_LIST_PATTERN = re.compile(r"^(positions|holdings|portfolio|assets)$")
# Pattern for top-level date key in structured files
_REPORT_DATE_PATTERN = re.compile(
    r"^(report_date|date|as_of_date|snapshot_date|effective_date|valuation_date)$"
)

# Date formats to try when parsing unknown files
_DATE_FORMATS = [
    "%Y-%m-%d", "%Y%m%d", "%m/%d/%Y", "%d/%m/%Y",
    "%m-%d-%Y", "%d-%m-%Y", "%Y/%m/%d",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date_flexible(value: str) -> date:
    """Try several common date formats."""
    value = value.strip().strip('"').strip("'")
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unable to parse date: '{value}'")


def _normalise_header(header: str) -> str:
    """Lowercase, strip, replace whitespace/hyphens with underscores."""
    return re.sub(r"[\s\-]+", "_", header.strip()).lower()


def _detect_delimiter(sample: str) -> str:
    """Use csv.Sniffer to guess the delimiter, default to comma."""
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",|\t;")
        return dialect.delimiter
    except csv.Error:
        return ","


def _map_keys(raw_keys: list[str], patterns: list[tuple[str, re.Pattern]]) -> tuple[dict[str, str], list[str]]:
    """
    Generic regex mapper – works for both tabular headers and YAML dict keys.

    Returns:
        mapping  – {raw_key: canonical_field}
        unmapped – list of raw keys that could not be resolved
    """
    mapping: dict[str, str] = {}
    unmapped: list[str] = []
    used_canonicals: set[str] = set()

    for raw in raw_keys:
        norm = _normalise_header(raw)
        matched = False
        for canon, pattern in patterns:
            if canon in used_canonicals:
                continue
            if pattern.search(norm):
                mapping[raw] = canon
                used_canonicals.add(canon)
                matched = True
                break
        if not matched:
            unmapped.append(raw)

    return mapping, unmapped


def _map_headers(raw_headers: list[str]) -> tuple[dict[str, str], list[str]]:
    """Map raw tabular headers to canonical trade fields."""
    return _map_keys(raw_headers, COLUMN_PATTERNS)


def _map_position_headers(raw_headers: list[str]) -> tuple[dict[str, str], list[str]]:
    """Map raw tabular headers to canonical position fields."""
    return _map_keys(raw_headers, POSITION_COLUMN_PATTERNS)


# ---------------------------------------------------------------------------
# Structured positions – YAML / JSON
# ---------------------------------------------------------------------------

def _ingest_structured_positions(file_content: str) -> dict:
    """
    Parse structured (YAML/JSON) position file into the positions table.

    Uses regex to resolve:
      - The top-level date key (report_date, as_of_date, date, …)
      - The positions list key (positions, holdings, portfolio, …)
      - Each position item's fields (account, ticker, shares, market_value, …)
    """
    data = yaml.safe_load(file_content)
    if not isinstance(data, dict):
        return _empty_pos_report(["File did not parse to a dict/object"])

    # --- Resolve top-level date key via regex ---
    report_date = None
    for key in data:
        if _REPORT_DATE_PATTERN.search(_normalise_header(str(key))):
            try:
                report_date = _parse_date_flexible(str(data[key]))
            except ValueError as exc:
                return _empty_pos_report([f"Invalid date in '{key}': {exc}"])
            break

    if report_date is None:
        return _empty_pos_report(["Could not find a report-date key in the file"])

    # --- Resolve positions list key via regex ---
    positions_list = None
    for key in data:
        if _POSITIONS_LIST_PATTERN.search(_normalise_header(str(key))):
            positions_list = data[key]
            break

    if not isinstance(positions_list, list):
        return _empty_pos_report(["Could not find a positions/holdings list in the file"])

    # --- Map item keys on the first item, then reuse ---
    if not positions_list:
        return _empty_pos_report(["Positions list is empty"])

    sample_keys = list(positions_list[0].keys())
    field_map, unmapped = _map_keys(sample_keys, POSITION_COLUMN_PATTERNS)
    canon_to_raw = {canon: raw for raw, canon in field_map.items()}
    resolved = set(canon_to_raw.keys())

    quality_issues: list[str] = []
    if unmapped:
        quality_issues.append(f"Unmapped position fields (ignored): {unmapped}")

    # We need at least account, ticker, shares, market_value
    missing = {"account_id", "ticker", "shares", "market_value"} - resolved
    if missing:
        quality_issues.append(
            f"Could not resolve required position fields: {sorted(missing)}. Resolved: {field_map}"
        )
        return {
            "format": "positions",
            "rows_inserted": 0,
            "rows_skipped_duplicate": 0,
            "invalid_rows": [],
            "quality_issues": quality_issues,
            "column_mapping": field_map,
        }

    inserted = skipped = 0
    invalid_rows: list[int] = []
    seen_tickers: set[str] = set()

    for row_num, pos in enumerate(positions_list, start=1):
        try:
            account_id = str(pos[canon_to_raw["account_id"]]).strip()
            ticker = str(pos[canon_to_raw["ticker"]]).strip().upper()
            shares = float(pos[canon_to_raw["shares"]])
            market_value = float(pos[canon_to_raw["market_value"]])
            seen_tickers.add(ticker)

            custodian_ref = None
            if "custodian_ref" in canon_to_raw:
                val = str(pos.get(canon_to_raw["custodian_ref"], "")).strip()
                custodian_ref = val or None

            existing = Position.query.filter_by(
                report_date=report_date,
                account_id=account_id,
                ticker=ticker,
            ).first()

            if existing:
                skipped += 1
                continue

            position = Position(
                report_date=report_date,
                account_id=account_id,
                ticker=ticker,
                shares=shares,
                market_value=market_value,
                custodian_ref=custodian_ref,
            )
            db.session.add(position)
            inserted += 1

        except (ValueError, KeyError) as exc:
            quality_issues.append(f"Position {row_num}: parse error – {exc}")
            invalid_rows.append(row_num)

    quality_issues.extend(validate_tickers(sorted(seen_tickers)))
    db.session.commit()
    return {
        "format": "positions",
        "rows_inserted": inserted,
        "rows_skipped_duplicate": skipped,
        "invalid_rows": invalid_rows,
        "quality_issues": quality_issues,
        "column_mapping": field_map,
    }


def _empty_pos_report(issues: list[str]) -> dict:
    return {
        "format": "positions",
        "rows_inserted": 0,
        "rows_skipped_duplicate": 0,
        "invalid_rows": [],
        "quality_issues": issues,
    }


# ---------------------------------------------------------------------------
# Tabular trade ingestion (auto-detect)
# ---------------------------------------------------------------------------

def _ingest_tabular_trades(file_content: str) -> dict:
    """
    Generic tabular trade ingester.

    1. Detect delimiter from the first few lines.
    2. Map headers to canonical fields via regex.
    3. Insert rows with whatever fields we can resolve.
    """
    sample = "\n".join(file_content.splitlines()[:5])
    delimiter = _detect_delimiter(sample)

    reader = csv.DictReader(io.StringIO(file_content), delimiter=delimiter)
    if not reader.fieldnames:
        return {
            "format": "auto_tabular",
            "rows_inserted": 0,
            "rows_skipped_duplicate": 0,
            "invalid_rows": [],
            "quality_issues": ["Could not detect any column headers"],
            "column_mapping": {},
        }

    col_map, unmapped = _map_headers(list(reader.fieldnames))
    # Invert: canonical -> raw_header
    canon_to_raw = {canon: raw for raw, canon in col_map.items()}
    resolved = set(canon_to_raw.keys())

    quality_issues: list[str] = []

    if unmapped:
        quality_issues.append(f"Unmapped columns (ignored): {unmapped}")

    missing_required = _REQUIRED_TRADE - resolved
    if missing_required:
        quality_issues.append(
            f"Could not resolve required columns: {sorted(missing_required)}. "
            f"Resolved: {col_map}"
        )
        return {
            "format": "auto_tabular",
            "rows_inserted": 0,
            "rows_skipped_duplicate": 0,
            "invalid_rows": [],
            "quality_issues": quality_issues,
            "column_mapping": col_map,
        }

    inserted = skipped = 0
    invalid_rows: list[int] = []
    seen_tickers: set[str] = set()

    for row_num, row in enumerate(reader, start=2):
        try:
            raw_date = row[canon_to_raw["trade_date"]].strip()
            trade_date = _parse_date_flexible(raw_date)
            account_id = row[canon_to_raw["account_id"]].strip()
            ticker = row[canon_to_raw["ticker"]].strip().upper()
            quantity = float(row[canon_to_raw["quantity"]])
            seen_tickers.add(ticker)

            price = None
            if "price" in canon_to_raw:
                val = row[canon_to_raw["price"]].strip()
                if val:
                    price = float(val)

            trade_type = None
            if "trade_type" in canon_to_raw:
                val = row[canon_to_raw["trade_type"]].strip().upper()
                if val:
                    trade_type = val
            if not trade_type:
                trade_type = "SELL" if quantity < 0 else "BUY"

            settlement_date = None
            if "settlement_date" in canon_to_raw:
                val = row[canon_to_raw["settlement_date"]].strip()
                if val:
                    settlement_date = _parse_date_flexible(val)

            market_value = None
            if "market_value" in canon_to_raw:
                val = row[canon_to_raw["market_value"]].strip()
                if val:
                    market_value = float(val)

            source_system = "internal"
            if "source_system" in canon_to_raw:
                val = row[canon_to_raw["source_system"]].strip()
                if val:
                    source_system = val

            # Deduplicate
            existing = Trade.query.filter_by(
                trade_date=trade_date,
                account_id=account_id,
                ticker=ticker,
                quantity=quantity,
                source_system=source_system,
            ).first()

            if existing:
                skipped += 1
                continue

            trade = Trade(
                trade_date=trade_date,
                account_id=account_id,
                ticker=ticker,
                quantity=quantity,
                price=price,
                trade_type=trade_type,
                settlement_date=settlement_date,
                market_value=market_value,
                source_system=source_system,
            )
            db.session.add(trade)
            inserted += 1

        except (ValueError, KeyError) as exc:
            quality_issues.append(f"Row {row_num}: parse error – {exc}")
            invalid_rows.append(row_num)

    quality_issues.extend(validate_tickers(sorted(seen_tickers)))
    db.session.commit()
    return {
        "format": "auto_tabular",
        "delimiter_detected": delimiter,
        "rows_inserted": inserted,
        "rows_skipped_duplicate": skipped,
        "invalid_rows": invalid_rows,
        "quality_issues": quality_issues,
        "column_mapping": col_map,
    }


# ---------------------------------------------------------------------------
# Tabular position ingestion (auto-detect)
# ---------------------------------------------------------------------------

def _ingest_tabular_positions(file_content: str) -> dict:
    """
    Generic tabular position ingester for CSV/pipe/tab-delimited position files.
    Maps headers via POSITION_COLUMN_PATTERNS and inserts into the positions table.
    """
    sample = "\n".join(file_content.splitlines()[:5])
    delimiter = _detect_delimiter(sample)

    reader = csv.DictReader(io.StringIO(file_content), delimiter=delimiter)
    if not reader.fieldnames:
        return _empty_pos_report(["Could not detect any column headers"])

    col_map, unmapped = _map_position_headers(list(reader.fieldnames))
    canon_to_raw = {canon: raw for raw, canon in col_map.items()}
    resolved = set(canon_to_raw.keys())

    quality_issues: list[str] = []
    if unmapped:
        quality_issues.append(f"Unmapped columns (ignored): {unmapped}")

    missing = _REQUIRED_POSITION - resolved
    if missing:
        quality_issues.append(
            f"Could not resolve required position columns: {sorted(missing)}. Resolved: {col_map}"
        )
        return {
            "format": "auto_positions",
            "rows_inserted": 0,
            "rows_skipped_duplicate": 0,
            "invalid_rows": [],
            "quality_issues": quality_issues,
            "column_mapping": col_map,
        }

    inserted = skipped = 0
    invalid_rows: list[int] = []
    seen_tickers: set[str] = set()

    for row_num, row in enumerate(reader, start=2):
        try:
            report_date = _parse_date_flexible(row[canon_to_raw["report_date"]])
            account_id = row[canon_to_raw["account_id"]].strip()
            ticker = row[canon_to_raw["ticker"]].strip().upper()
            shares = float(row[canon_to_raw["shares"]])
            market_value = float(row[canon_to_raw["market_value"]])
            seen_tickers.add(ticker)

            custodian_ref = None
            if "custodian_ref" in canon_to_raw:
                val = row[canon_to_raw["custodian_ref"]].strip()
                custodian_ref = val or None

            existing = Position.query.filter_by(
                report_date=report_date,
                account_id=account_id,
                ticker=ticker,
            ).first()

            if existing:
                skipped += 1
                continue

            position = Position(
                report_date=report_date,
                account_id=account_id,
                ticker=ticker,
                shares=shares,
                market_value=market_value,
                custodian_ref=custodian_ref,
            )
            db.session.add(position)
            inserted += 1

        except (ValueError, KeyError) as exc:
            quality_issues.append(f"Row {row_num}: parse error – {exc}")
            invalid_rows.append(row_num)

    quality_issues.extend(validate_tickers(sorted(seen_tickers)))
    db.session.commit()
    return {
        "format": "auto_positions",
        "delimiter_detected": delimiter,
        "rows_inserted": inserted,
        "rows_skipped_duplicate": skipped,
        "invalid_rows": invalid_rows,
        "quality_issues": quality_issues,
        "column_mapping": col_map,
    }


# ---------------------------------------------------------------------------
# Classification heuristic
# ---------------------------------------------------------------------------

def _is_structured_positions(data) -> bool:
    """Return True if parsed YAML/JSON looks like a positions file."""
    if not isinstance(data, dict):
        return False
    for key in data:
        if _POSITIONS_LIST_PATTERN.search(_normalise_header(str(key))):
            return isinstance(data[key], list)
    return False


def _classify_tabular(headers: list[str]) -> str:
    """
    Decide whether a tabular file is trades or positions by mapping headers
    against both pattern sets and checking which side-specific fields resolved.

    Heuristic:
      - trade-only fields:    price, trade_type, settlement_date
      - position-only fields: custodian_ref
      - If trade-only fields found → trades
      - If position-only fields found and no trade-only → positions
      - Ambiguous → default to trades
    """
    trade_map, _ = _map_keys(headers, COLUMN_PATTERNS)
    pos_map, _ = _map_keys(headers, POSITION_COLUMN_PATTERNS)
    trade_resolved = set(trade_map.values())
    pos_resolved = set(pos_map.values())

    has_trade_only = bool(trade_resolved & _TRADE_ONLY_FIELDS)
    has_pos_only = bool(pos_resolved & _POSITION_ONLY_FIELDS)

    if has_pos_only and not has_trade_only:
        return "positions"
    return "trades"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ingest_auto(file_content: str) -> dict:
    """
    Auto-detect file format and ingest.

    - Structured (YAML/JSON) with a positions/holdings list  →  bank positions
    - Tabular with position-specific headers                 →  tabular positions
    - Tabular otherwise                                      →  tabular trades
    """
    stripped = file_content.strip()

    # --- Try structured formats (YAML / JSON) first ---
    parsed = None
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            parsed = json.loads(stripped)
        except (json.JSONDecodeError, ValueError):
            pass

    if parsed is None:
        try:
            parsed = yaml.safe_load(stripped)
        except yaml.YAMLError:
            pass

    if parsed is not None and _is_structured_positions(parsed):
        return _ingest_structured_positions(file_content)

    # --- Tabular / delimited ---
    # Peek at headers to decide trades vs positions
    sample = "\n".join(stripped.splitlines()[:5])
    delimiter = _detect_delimiter(sample)
    reader = csv.DictReader(io.StringIO(stripped), delimiter=delimiter)
    if reader.fieldnames and _classify_tabular(list(reader.fieldnames)) == "positions":
        return _ingest_tabular_positions(file_content)

    return _ingest_tabular_trades(file_content)
