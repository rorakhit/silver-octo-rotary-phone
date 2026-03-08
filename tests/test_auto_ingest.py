"""Tests for auto-detection ingestion via POST /ingest with file key."""

import io

import pytest


class TestAutoDetectColumnMapping:
    """The auto-ingester should handle arbitrary header names via regex."""

    def test_auto_csv_with_nonstandard_headers(self, client):
        """Totally different column names that still match our regex patterns."""
        csv = (
            "Transaction_Date,Acct,Symbol,Qty,Px,Side,Value_Date\n"
            "2025-03-01,ACC_X,AAPL,50,190.00,BUY,2025-03-03\n"
            "2025-03-01,ACC_X,MSFT,25,430.00,SELL,2025-03-03\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(csv.encode()), "random_trades.csv"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        assert resp.status_code == 200
        report = data["reports"][0]
        assert report["format"] == "auto_tabular"
        assert report["rows_inserted"] == 2
        # Verify column mapping resolved correctly
        mapping = report["column_mapping"]
        assert mapping["Transaction_Date"] == "trade_date"
        assert mapping["Symbol"] == "ticker"
        assert mapping["Qty"] == "quantity"
        assert mapping["Px"] == "price"
        assert mapping["Side"] == "trade_type"

    def test_auto_tab_delimited(self, client):
        """Tab-separated file should be detected and parsed."""
        tsv = (
            "Date\tAccount\tTicker\tShares\tPrice\n"
            "2025-04-01\tACCT_TAB\tGOOGL\t30\t155.00\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(tsv.encode()), "trades.tsv"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["rows_inserted"] == 1
        assert report["delimiter_detected"] == "\t"

    def test_auto_pipe_delimited(self, client):
        """Pipe-delimited with unusual headers."""
        pipe = (
            "Exec_Date|Portfolio|Instrument|Units|Execution_Price|Direction\n"
            "01/15/2025|ACC_PIPE|TSLA|100|245.00|BUY\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(pipe.encode()), "custodian.dat"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["rows_inserted"] == 1
        mapping = report["column_mapping"]
        assert mapping["Instrument"] == "ticker"
        assert mapping["Units"] == "quantity"

    def test_auto_semicolon_delimited(self, client):
        """Semicolon-separated file."""
        semi = (
            "trade_dt;acct_id;security;qty;px\n"
            "2025-05-01;ACC_SEMI;NVDA;40;510.00\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(semi.encode()), "data.csv"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["rows_inserted"] == 1
        assert report["delimiter_detected"] == ";"

    def test_auto_derives_trade_type_from_negative_qty(self, client):
        """When no trade_type column exists, negative qty = SELL."""
        csv = (
            "Date,Account,Ticker,Quantity,Price\n"
            "2025-06-01,ACCT_NEG,AAPL,-50,195.00\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(csv.encode()), "sells.csv"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        assert data["reports"][0]["rows_inserted"] == 1
        # Verify in DB
        from app.models import Trade
        with client.application.app_context():
            trade = Trade.query.filter_by(
                account_id="ACCT_NEG", ticker="AAPL"
            ).first()
            assert trade.trade_type == "SELL"

    def test_auto_yaml_positions_detected(self, client):
        """YAML with 'positions' key should route to positions ingester."""
        yaml_str = (
            "report_date: '20250601'\n"
            "positions:\n"
            "  - account_id: 'ACCT_AUTO'\n"
            "    ticker: 'GOOG'\n"
            "    shares: 10\n"
            "    market_value: 1500.00\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(yaml_str.encode()), "positions.yaml"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["format"] == "positions"
        assert report["rows_inserted"] == 1

    def test_auto_json_positions_detected(self, client):
        """JSON with 'positions' key should route to positions ingester."""
        import json
        payload = {
            "report_date": "20250701",
            "positions": [
                {"account_id": "ACC_JSON", "ticker": "META", "shares": 20, "market_value": 5000.0}
            ]
        }
        # JSON is valid YAML, so this should still work through yaml.safe_load
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(json.dumps(payload).encode()), "positions.json"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["format"] == "positions"
        assert report["rows_inserted"] == 1

    def test_auto_unmappable_columns_reported(self, client):
        """Columns that can't be mapped should appear in quality_issues."""
        csv = (
            "Date,Account,Ticker,Quantity,WeirdColumn1,WeirdColumn2\n"
            "2025-07-01,ACCT_UNK,AAPL,10,foo,bar\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(csv.encode()), "unknown_cols.csv"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["rows_inserted"] == 1
        # Unmapped columns flagged
        unmapped_issues = [q for q in report["quality_issues"] if "Unmapped" in q]
        assert len(unmapped_issues) > 0
        assert "WeirdColumn1" in unmapped_issues[0]

    def test_auto_missing_required_columns_rejected(self, client):
        """File missing required columns should fail gracefully."""
        csv = (
            "WeirdA,WeirdB,WeirdC\n"
            "1,2,3\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(csv.encode()), "garbage.csv"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["rows_inserted"] == 0
        assert any("Could not resolve required" in q for q in report["quality_issues"])

    def test_auto_multiple_date_formats(self, client):
        """Various date formats should all be parsed."""
        csv = (
            "Date,Account,Ticker,Qty\n"
            "03/15/2025,ACCT_DT1,AAPL,10\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(csv.encode()), "dates.csv"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        assert data["reports"][0]["rows_inserted"] == 1

    def test_auto_deduplicates(self, client):
        """Sending the same file twice should not double-insert."""
        csv = (
            "Date,Account,Ticker,Quantity\n"
            "2025-08-01,ACCT_DUP,MSFT,100\n"
        )
        client.post("/ingest", data={
            "file": (io.BytesIO(csv.encode()), "dup.csv"),
        }, content_type="multipart/form-data")
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(csv.encode()), "dup.csv"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["rows_inserted"] == 0
        assert report["rows_skipped_duplicate"] == 1


class TestAnyKeyNameWorks:
    """Files uploaded under any key name should be auto-detected."""

    def test_arbitrary_key_name(self, client):
        csv = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-09-01,ACCT_EXPL,AAPL,5,200.00,BUY,2025-09-03\n"
        )
        resp = client.post("/ingest", data={
            "my_custom_key": (io.BytesIO(csv.encode()), "custom.csv"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        assert data["reports"][0]["format"] == "auto_tabular"
        assert data["reports"][0]["rows_inserted"] == 1
