"""
Ticker validation against SEC EDGAR (US), local DB, and OpenFIGI (international).

Validation is three-tier, ordered from fastest to slowest lookup:

  1. SEC bulk cache — ~10K US-listed tickers, refreshed daily from
     https://www.sec.gov/files/company_tickers.json.
     After the first load this is an in-memory set → O(1) lookup.
     Catches the vast majority of tickers since most ingested data
     will reference US-listed equities.

  2. Local DB fallback — tickers already ingested into our trades/positions
     tables are implicitly valid (no external call needed).  Requires a
     SQL query per call, so it's slower than the in-memory SEC set but
     avoids a network round-trip.  Useful for international tickers
     (e.g. VOW3.DE) that wouldn't appear in the SEC list.

  3. OpenFIGI fallback — for tickers not in SEC or DB, a per-ticker
     lookup via https://api.openfigi.com/v3/mapping (no API key needed).
     Results are cached to disk so each ticker is only queried once.
     Slowest tier: network call, used only as a last resort.

The tiers are intentionally ordered fastest → slowest to minimise
latency.  Checking the DB first would incur a SQL query even for
common US tickers that resolve instantly from the in-memory set.

All layers fail-open: if the network is unavailable, validation passes
everything rather than blocking ingestion.  Ticker warnings are
advisory only — they appear in the data-quality report but never
prevent rows from being inserted.

Usage:
    from app.services.ticker_registry import validate_tickers

    warnings = validate_tickers(["AAPL", "VOW3.DE", "XYZ123"])
    # ["Ticker 'XYZ123' not found in SEC, DB, or OpenFIGI registries"]
"""

import json
import logging
import os
import time
from urllib.error import URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

_CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", ".ticker_cache")
_SEC_CACHE_FILE = os.path.join(_CACHE_DIR, "sec_tickers.json")
_FIGI_CACHE_FILE = os.path.join(_CACHE_DIR, "openfigi_cache.json")
_META_FILE = os.path.join(_CACHE_DIR, "meta.json")

_SEC_URL = "https://www.sec.gov/files/company_tickers.json"
_OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"

TICKER_CACHE_TTL_SECONDS = 86400  # 24 hours

# In-memory caches, populated lazily
_sec_ticker_set: set[str] | None = None
_figi_cache: dict[str, bool] | None = None  # ticker -> True (valid) / False (unknown)


# ---------------------------------------------------------------------------
# SEC bulk cache
# ---------------------------------------------------------------------------

def _sec_cache_is_fresh() -> bool:
    """Check if the local SEC ticker cache is within its 24-hour TTL."""
    if not os.path.exists(_META_FILE) or not os.path.exists(_SEC_CACHE_FILE):
        return False
    try:
        with open(_META_FILE) as f:
            meta = json.load(f)
        fetched_at = meta.get("sec_fetched_at", 0)
        return (time.time() - fetched_at) < TICKER_CACHE_TTL_SECONDS
    except (json.JSONDecodeError, OSError):
        return False


def _fetch_sec_tickers() -> set[str]:
    """Download SEC tickers JSON and write to local cache."""
    os.makedirs(_CACHE_DIR, exist_ok=True)

    req = Request(_SEC_URL)
    req.add_header("User-Agent", "portfolio-clearinghouse/1.0")
    req.add_header("Accept", "application/json")

    try:
        with urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
    except (URLError, OSError) as exc:
        logger.warning("Failed to fetch SEC tickers: %s", exc)
        return set()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("SEC tickers response was not valid JSON")
        return set()

    tickers = {entry["ticker"].upper() for entry in data.values() if "ticker" in entry}

    with open(_SEC_CACHE_FILE, "w") as f:
        json.dump(sorted(tickers), f)

    # Update meta
    meta = _load_meta()
    meta["sec_fetched_at"] = time.time()
    meta["sec_count"] = len(tickers)
    _save_meta(meta)

    logger.info("Cached %d SEC tickers", len(tickers))
    return tickers


def _load_sec_from_cache() -> set[str]:
    try:
        with open(_SEC_CACHE_FILE) as f:
            return set(json.load(f))
    except (json.JSONDecodeError, OSError):
        return set()


def get_sec_ticker_set() -> set[str]:
    """
    Return the set of SEC-listed tickers, fetching/refreshing as needed.

    Lazy-loads: the first call fetches from SEC (or disk cache), then
    subsequent calls return the in-memory set with zero overhead.
    If a fresh fetch fails, falls back to a stale cache rather than
    returning an empty set (which would cause fail-open on all tickers).
    """
    global _sec_ticker_set

    if _sec_ticker_set is not None:
        return _sec_ticker_set

    if _sec_cache_is_fresh():
        _sec_ticker_set = _load_sec_from_cache()
    else:
        _sec_ticker_set = _fetch_sec_tickers()
        if not _sec_ticker_set and os.path.exists(_SEC_CACHE_FILE):
            # Network failed but we have an old cache — better stale than empty
            logger.warning("Using stale SEC ticker cache as fallback")
            _sec_ticker_set = _load_sec_from_cache()

    return _sec_ticker_set


# ---------------------------------------------------------------------------
# OpenFIGI per-ticker lookup with local cache
# ---------------------------------------------------------------------------

def _load_figi_cache() -> dict[str, bool]:
    """Load the OpenFIGI results cache from disk."""
    global _figi_cache
    if _figi_cache is not None:
        return _figi_cache
    try:
        with open(_FIGI_CACHE_FILE) as f:
            _figi_cache = json.load(f)
    except (json.JSONDecodeError, OSError, FileNotFoundError):
        _figi_cache = {}
    return _figi_cache


def _save_figi_cache():
    """Persist the OpenFIGI results cache to disk."""
    if _figi_cache is None:
        return
    os.makedirs(_CACHE_DIR, exist_ok=True)
    with open(_FIGI_CACHE_FILE, "w") as f:
        json.dump(_figi_cache, f)


def _lookup_openfigi(tickers: list[str]) -> dict[str, bool]:
    """
    Query OpenFIGI for a batch of tickers.

    OpenFIGI accepts up to 100 items per request (without API key).
    Returns {ticker: True/False} for each queried ticker.
    """
    if not tickers:
        return {}

    results: dict[str, bool] = {}

    # OpenFIGI allows max 10 items per request without API key
    batch_size = 10
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        payload = [{"idType": "TICKER", "idValue": t} for t in batch]

        req = Request(
            _OPENFIGI_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "portfolio-clearinghouse/1.0",
            },
            method="POST",
        )

        try:
            with urlopen(req, timeout=15) as resp:
                response_data = json.loads(resp.read().decode("utf-8"))
        except (URLError, OSError, json.JSONDecodeError) as exc:
            logger.warning("OpenFIGI lookup failed for batch: %s", exc)
            # Fail-open: treat as valid if we can't reach the API
            for t in batch:
                results[t] = True
            continue

        for ticker, entry in zip(batch, response_data):
            if "data" in entry and len(entry["data"]) > 0:
                results[ticker] = True
            else:
                results[ticker] = False

    return results


def _check_openfigi(tickers: list[str]) -> dict[str, bool]:
    """
    Check tickers against OpenFIGI, using local cache first.
    Only queries the API for tickers not already cached.
    """
    cache = _load_figi_cache()
    uncached = [t for t in tickers if t not in cache]

    if uncached:
        fresh = _lookup_openfigi(uncached)
        cache.update(fresh)
        _save_figi_cache()

    return {t: cache.get(t, True) for t in tickers}


# ---------------------------------------------------------------------------
# Local DB fallback
# ---------------------------------------------------------------------------

def _get_db_tickers() -> set[str]:
    """
    Return the set of distinct tickers already stored in our trades and
    positions tables.  Uses no_autoflush so that pending (uncommitted)
    inserts from the current ingestion batch are not visible — only
    previously committed tickers count as "known".

    Returns an empty set if the app context or tables are unavailable
    (fail-open).
    """
    try:
        from flask import current_app
        if not current_app:
            return set()
        from app import db
        from app.models import Trade, Position
        with db.session.no_autoflush:
            trade_tickers = {r[0].upper() for r in db.session.query(Trade.ticker).distinct().all()}
            pos_tickers = {r[0].upper() for r in db.session.query(Position.ticker).distinct().all()}
        return trade_tickers | pos_tickers
    except Exception:
        return set()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_ticker(ticker: str) -> bool:
    """
    Return True if the ticker is recognized by SEC, our DB, or OpenFIGI.
    Returns True if all sources are unavailable (fail-open).
    """
    t = ticker.upper()
    sec = get_sec_ticker_set()

    # Fast path: found in SEC
    if sec and t in sec:
        return True

    # If SEC is unavailable and no cache, fail-open
    if not sec:
        return True

    # DB fallback: already ingested = valid
    if t in _get_db_tickers():
        return True

    # Final fallback: check OpenFIGI
    results = _check_openfigi([t])
    return results.get(t, True)


def validate_tickers(tickers: list[str]) -> list[str]:
    """
    Validate a list of tickers against SEC, local DB, and OpenFIGI.

    Returns a list of warning strings for tickers not found in any source.
    Returns an empty list if registries are unavailable (fail-open).

    The validation cascade progressively narrows the set of unknowns
    through each tier, so expensive lookups (DB query, network call)
    only run for tickers not already resolved by a cheaper tier.
    """
    if not tickers:
        return []

    # --- Tier 1: SEC in-memory set (O(1) per ticker) ---
    sec = get_sec_ticker_set()

    # If SEC is completely unavailable, fail-open — no warnings
    if not sec:
        return []

    # Only tickers NOT in the SEC set need further checking
    unknown_in_sec = [t.upper() for t in tickers if t.upper() not in sec]

    if not unknown_in_sec:
        return []

    # --- Tier 2: local DB (SQL query, catches previously-ingested tickers) ---
    db_tickers = _get_db_tickers()
    still_unknown = [t for t in unknown_in_sec if t not in db_tickers]

    if not still_unknown:
        return []

    # --- Tier 3: OpenFIGI (network call, last resort) ---
    figi_results = _check_openfigi(still_unknown)

    warnings = []
    for t in still_unknown:
        if not figi_results.get(t, True):
            warnings.append(f"Ticker '{t}' not found in SEC, DB, or OpenFIGI registries")

    return warnings


# ---------------------------------------------------------------------------
# Utilities (testing, cache management)
# ---------------------------------------------------------------------------

def _load_meta() -> dict:
    try:
        with open(_META_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError, FileNotFoundError):
        return {}


def _save_meta(meta: dict):
    os.makedirs(_CACHE_DIR, exist_ok=True)
    with open(_META_FILE, "w") as f:
        json.dump(meta, f)


def clear_cache():
    """Remove cached files and in-memory state. Useful for testing."""
    global _sec_ticker_set, _figi_cache
    _sec_ticker_set = None
    _figi_cache = None
    for path in (_SEC_CACHE_FILE, _FIGI_CACHE_FILE, _META_FILE):
        if os.path.exists(path):
            os.remove(path)
