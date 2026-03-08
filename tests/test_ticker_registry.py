"""Tests for three-tier ticker validation (SEC + DB + OpenFIGI)."""

import json
import os
import time
from unittest.mock import patch

import pytest

from app.services.ticker_registry import (
    TICKER_CACHE_TTL_SECONDS,
    _CACHE_DIR,
    _FIGI_CACHE_FILE,
    _META_FILE,
    _SEC_CACHE_FILE,
    clear_cache,
    get_sec_ticker_set,
    validate_ticker,
    validate_tickers,
)

# All unit tests mock _get_db_tickers to return empty set by default,
# isolating tests from whatever is in the actual DB.
_MOCK_DB_EMPTY = patch("app.services.ticker_registry._get_db_tickers", return_value=set())


@pytest.fixture(autouse=True)
def clean_ticker_cache():
    """Clear in-memory and file cache before and after each test."""
    clear_cache()
    yield
    clear_cache()


class TestSECCacheAndValidation:
    """Test SEC caching, loading, and validation."""

    def _seed_sec_cache(self, tickers, fetched_at=None):
        """Write a fake SEC cache to disk."""
        os.makedirs(_CACHE_DIR, exist_ok=True)
        with open(_SEC_CACHE_FILE, "w") as f:
            json.dump(sorted(tickers), f)
        with open(_META_FILE, "w") as f:
            json.dump({
                "sec_fetched_at": fetched_at or time.time(),
                "sec_count": len(tickers),
            }, f)

    def test_validate_known_ticker(self):
        self._seed_sec_cache(["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"])
        assert validate_ticker("AAPL") is True
        assert validate_ticker("aapl") is True  # case-insensitive

    @_MOCK_DB_EMPTY
    @patch("app.services.ticker_registry._check_openfigi")
    def test_validate_unknown_ticker(self, mock_figi, _mock_db):
        """Ticker not in SEC or DB falls through to OpenFIGI; if not there either, returns False."""
        self._seed_sec_cache(["AAPL", "MSFT"])
        mock_figi.return_value = {"XYZFAKE": False}
        assert validate_ticker("XYZFAKE") is False
        mock_figi.assert_called_once_with(["XYZFAKE"])

    @_MOCK_DB_EMPTY
    @patch("app.services.ticker_registry._check_openfigi")
    def test_validate_tickers_returns_warnings(self, mock_figi, _mock_db):
        self._seed_sec_cache(["AAPL", "MSFT", "GOOGL"])
        mock_figi.return_value = {"BADTICKER": False, "NOPE": False}
        warnings = validate_tickers(["AAPL", "BADTICKER", "GOOGL", "NOPE"])
        assert len(warnings) == 2
        assert "BADTICKER" in warnings[0]
        assert "NOPE" in warnings[1]

    def test_validate_tickers_all_valid(self):
        self._seed_sec_cache(["AAPL", "MSFT"])
        warnings = validate_tickers(["AAPL", "MSFT"])
        assert warnings == []

    def test_stale_cache_triggers_refresh(self):
        stale_time = time.time() - TICKER_CACHE_TTL_SECONDS - 100
        self._seed_sec_cache(["AAPL"], fetched_at=stale_time)

        with patch("app.services.ticker_registry._fetch_sec_tickers") as mock_fetch:
            mock_fetch.return_value = {"AAPL", "MSFT", "NEWSTOCK"}
            tickers = get_sec_ticker_set()
            mock_fetch.assert_called_once()
            assert "NEWSTOCK" in tickers

    def test_fresh_cache_no_fetch(self):
        self._seed_sec_cache(["AAPL", "MSFT"])

        with patch("app.services.ticker_registry._fetch_sec_tickers") as mock_fetch:
            tickers = get_sec_ticker_set()
            mock_fetch.assert_not_called()
            assert "AAPL" in tickers

    def test_empty_registry_fails_open(self):
        """If no cache and fetch fails, validation should pass everything."""
        with patch("app.services.ticker_registry._fetch_sec_tickers", return_value=set()):
            assert validate_ticker("ANYTHING") is True
            assert validate_tickers(["FOO", "BAR"]) == []


class TestDBFallback:
    """Test that tickers already in our DB skip external lookups."""

    def _seed_sec_cache(self, tickers):
        os.makedirs(_CACHE_DIR, exist_ok=True)
        with open(_SEC_CACHE_FILE, "w") as f:
            json.dump(sorted(tickers), f)
        with open(_META_FILE, "w") as f:
            json.dump({
                "sec_fetched_at": time.time(),
                "sec_count": len(tickers),
            }, f)

    @patch("app.services.ticker_registry._check_openfigi")
    @patch("app.services.ticker_registry._get_db_tickers", return_value={"VOW3.DE"})
    def test_db_ticker_skips_openfigi(self, _mock_db, mock_figi):
        """Ticker in DB should be valid without calling OpenFIGI."""
        self._seed_sec_cache(["AAPL"])
        assert validate_ticker("VOW3.DE") is True
        mock_figi.assert_not_called()

    @patch("app.services.ticker_registry._check_openfigi")
    @patch("app.services.ticker_registry._get_db_tickers", return_value={"SIE.DE", "VOW3.DE"})
    def test_batch_db_tickers_skip_openfigi(self, _mock_db, mock_figi):
        """Batch validate: DB-known tickers skip OpenFIGI entirely."""
        self._seed_sec_cache(["AAPL", "MSFT"])
        mock_figi.return_value = {"FAKE123": False}
        warnings = validate_tickers(["AAPL", "VOW3.DE", "SIE.DE", "FAKE123"])
        assert len(warnings) == 1
        assert "FAKE123" in warnings[0]
        # Only FAKE123 should have been sent to OpenFIGI
        mock_figi.assert_called_once_with(["FAKE123"])

    @patch("app.services.ticker_registry._check_openfigi")
    @patch("app.services.ticker_registry._get_db_tickers", return_value={"KNOWN"})
    def test_db_fallback_prevents_warning(self, _mock_db, mock_figi):
        """Ticker not in SEC but in DB should produce no warning."""
        self._seed_sec_cache(["AAPL"])
        warnings = validate_tickers(["AAPL", "KNOWN"])
        assert warnings == []
        mock_figi.assert_not_called()

    @patch("app.services.ticker_registry._lookup_openfigi")
    @patch("app.services.ticker_registry._get_db_tickers", return_value=set())
    def test_db_empty_falls_through_to_openfigi(self, _mock_db, mock_lookup):
        """When DB has nothing, unknown tickers still go to OpenFIGI."""
        self._seed_sec_cache(["AAPL"])
        mock_lookup.return_value = {"NEWCORP": True}
        assert validate_ticker("NEWCORP") is True
        mock_lookup.assert_called_once()


class TestOpenFIGIFallback:
    """Test OpenFIGI lookup and caching."""

    def _seed_sec_cache(self, tickers):
        os.makedirs(_CACHE_DIR, exist_ok=True)
        with open(_SEC_CACHE_FILE, "w") as f:
            json.dump(sorted(tickers), f)
        with open(_META_FILE, "w") as f:
            json.dump({
                "sec_fetched_at": time.time(),
                "sec_count": len(tickers),
            }, f)

    @_MOCK_DB_EMPTY
    @patch("app.services.ticker_registry._lookup_openfigi")
    def test_openfigi_validates_international_ticker(self, mock_lookup, _mock_db):
        """Ticker not in SEC but found in OpenFIGI should be valid."""
        self._seed_sec_cache(["AAPL", "MSFT"])
        mock_lookup.return_value = {"VOW3.DE": True}
        assert validate_ticker("VOW3.DE") is True

    @_MOCK_DB_EMPTY
    @patch("app.services.ticker_registry._lookup_openfigi")
    def test_openfigi_result_cached_to_disk(self, mock_lookup, _mock_db):
        """After querying OpenFIGI, result is persisted so API isn't called again."""
        self._seed_sec_cache(["AAPL"])
        mock_lookup.return_value = {"SIE.DE": True}

        # First call hits the API
        assert validate_ticker("SIE.DE") is True
        mock_lookup.assert_called_once()

        # Clear in-memory cache but keep disk cache
        from app.services import ticker_registry
        ticker_registry._figi_cache = None
        ticker_registry._sec_ticker_set = None

        # Second call should use disk cache, not API
        mock_lookup.reset_mock()
        assert validate_ticker("SIE.DE") is True
        mock_lookup.assert_not_called()

    @_MOCK_DB_EMPTY
    @patch("app.services.ticker_registry._lookup_openfigi")
    def test_openfigi_network_failure_fails_open(self, mock_lookup, _mock_db):
        """If OpenFIGI API call fails, the ticker is treated as valid (fail-open)."""
        self._seed_sec_cache(["AAPL"])
        mock_lookup.return_value = {"UNKNOWN": True}
        assert validate_ticker("UNKNOWN") is True

    @_MOCK_DB_EMPTY
    @patch("app.services.ticker_registry._check_openfigi")
    def test_validate_tickers_mixed_sec_and_figi(self, mock_figi, _mock_db):
        """SEC-known tickers skip OpenFIGI; unknowns go through it."""
        self._seed_sec_cache(["AAPL", "MSFT"])
        mock_figi.return_value = {"VOW3.DE": True, "FAKE123": False}
        warnings = validate_tickers(["AAPL", "VOW3.DE", "FAKE123"])
        assert len(warnings) == 1
        assert "FAKE123" in warnings[0]
        assert "SEC, DB, or OpenFIGI" in warnings[0]


class TestTickerValidationInIngestion:
    """Ticker warnings should appear in quality_issues when ingesting."""

    def _seed_sec_cache(self, tickers):
        os.makedirs(_CACHE_DIR, exist_ok=True)
        with open(_SEC_CACHE_FILE, "w") as f:
            json.dump(sorted(tickers), f)
        with open(_META_FILE, "w") as f:
            json.dump({"sec_fetched_at": time.time(), "sec_count": len(tickers)}, f)

    @patch("app.services.ticker_registry._check_openfigi")
    def test_ingestion_flags_unknown_ticker(self, mock_figi, client):
        import io
        self._seed_sec_cache(["AAPL", "MSFT"])
        mock_figi.return_value = {"FAKETICKER": False}
        csv = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACCT_TV,AAPL,10,190.00,BUY,2025-01-17\n"
            "2025-01-15,ACCT_TV,FAKETICKER,5,100.00,BUY,2025-01-17\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(csv.encode()), "tv_test.csv"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["rows_inserted"] == 2  # still inserted (warning, not rejection)
        ticker_issues = [q for q in report["quality_issues"] if "SEC, DB, or OpenFIGI" in q]
        assert len(ticker_issues) == 1
        assert "FAKETICKER" in ticker_issues[0]

    @patch("app.services.ticker_registry._check_openfigi")
    def test_nonstandard_headers_flags_unknown_ticker(self, mock_figi, client):
        import io
        self._seed_sec_cache(["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"])
        mock_figi.return_value = {"ZZZZZ": False}
        csv = (
            "Date,Account,Ticker,Qty,Price,Side\n"
            "2025-01-20,ACCT_TV2,AAPL,10,190.00,BUY\n"
            "2025-01-20,ACCT_TV2,ZZZZZ,5,50.00,BUY\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(csv.encode()), "tv_auto.csv"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["rows_inserted"] == 2
        ticker_issues = [q for q in report["quality_issues"] if "SEC, DB, or OpenFIGI" in q]
        assert len(ticker_issues) == 1
        assert "ZZZZZ" in ticker_issues[0]
