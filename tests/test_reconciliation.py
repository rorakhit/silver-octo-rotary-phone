"""Tests for GET /reconciliation endpoint."""


class TestReconciliationEndpoint:
    def test_reconciliation_requires_date(self, client):
        resp = client.get("/reconciliation")
        assert resp.status_code == 400

    def test_reconciliation_invalid_date(self, client):
        resp = client.get("/reconciliation?date=bad-date")
        assert resp.status_code == 400

    def test_reconciliation_no_data_returns_empty(self, client):
        resp = client.get("/reconciliation?date=1999-01-01")
        data = resp.get_json()
        assert data["discrepancies"] == []

    def test_reconciliation_returns_summary(self, client):
        resp = client.get("/reconciliation?date=2025-01-15")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "summary" in data
        assert "discrepancies" in data
        for key in ("matched", "shares_mismatch", "missing_in_bank", "missing_in_trades"):
            assert key in data["summary"]

    def test_reconciliation_detects_shares_mismatch(self, client):
        """
        Format2 ACC001 GOOGL = 100 shares; Bank ACC001 GOOGL = 75 shares => mismatch
        """
        resp = client.get("/reconciliation?date=2025-01-15")
        data = resp.get_json()
        mismatch = next(
            (d for d in data["discrepancies"]
             if d["type"] == "shares_mismatch"
             and d["account_id"] == "ACC001"
             and d["ticker"] == "GOOGL"),
            None
        )
        assert mismatch is not None
        assert mismatch["custodian_shares"] == 100.0
        assert mismatch["bank_shares"] == 75.0
        assert mismatch["share_difference"] == 25.0

    def test_reconciliation_detects_missing_in_bank(self, client):
        """
        Format2 has ACC004 AAPL and MSFT; bank positions have no ACC004 entries.
        """
        resp = client.get("/reconciliation?date=2025-01-15")
        data = resp.get_json()
        missing_in_bank = [d for d in data["discrepancies"] if d["type"] == "missing_in_bank"]
        account_tickers = {(d["account_id"], d["ticker"]) for d in missing_in_bank}
        assert ("ACC004", "AAPL") in account_tickers

    def test_reconciliation_detects_missing_in_trades(self, client):
        """
        Bank has ACC002 TSLA; Format2 does not.
        """
        resp = client.get("/reconciliation?date=2025-01-15")
        data = resp.get_json()
        missing_in_trades = [d for d in data["discrepancies"] if d["type"] == "missing_in_trades"]
        account_tickers = {(d["account_id"], d["ticker"]) for d in missing_in_trades}
        assert ("ACC002", "TSLA") in account_tickers

    def test_reconciliation_summary_counts_are_consistent(self, client):
        resp = client.get("/reconciliation?date=2025-01-15")
        data = resp.get_json()
        summary = data["summary"]
        discrepancies = data["discrepancies"]
        assert summary["shares_mismatch"] == sum(1 for d in discrepancies if d["type"] == "shares_mismatch")
        assert summary["missing_in_bank"] == sum(1 for d in discrepancies if d["type"] == "missing_in_bank")
        assert summary["missing_in_trades"] == sum(1 for d in discrepancies if d["type"] == "missing_in_trades")
