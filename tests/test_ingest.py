"""Tests for POST /ingest endpoint and ingestion service."""

import io
import pytest


class TestIngestEndpoint:
    def test_ingest_returns_200(self, client):
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(b"TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"), "t.csv"),
        }, content_type="multipart/form-data")
        assert resp.status_code == 200

    def test_ingest_no_files_returns_400(self, client):
        resp = client.post("/ingest", data={}, content_type="multipart/form-data")
        assert resp.status_code == 400

    def test_ingest_multiple_files(self, client):
        resp = client.post("/ingest", data={
            "trades": (io.BytesIO(b"TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"), "trades.csv"),
            "custodian": (io.BytesIO(b"REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"), "custodian.csv"),
            "positions": (io.BytesIO(b"report_date: '20250115'\npositions: []\n"), "pos.yaml"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert len(data["reports"]) == 3

    def test_ingest_detects_missing_required_columns(self, client):
        """File without any recognisable required columns should report quality issues."""
        bad_csv = (
            "WeirdA,WeirdB,WeirdC\n"
            "1,2,3\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(bad_csv.encode()), "bad.csv"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert len(report["quality_issues"]) > 0
        assert report["rows_inserted"] == 0

    def test_ingest_deduplicates_records(self, client, app):
        """Second ingest of same data should not increase row count."""
        from app.models import Trade
        with app.app_context():
            from app import db
            count_before = Trade.query.count()

        csv = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
        )
        client.post("/ingest", data={
            "file": (io.BytesIO(csv.encode()), "dup.csv"),
        }, content_type="multipart/form-data")

        with app.app_context():
            count_after = Trade.query.count()

        assert count_before == count_after


class TestIngestionService:
    def test_csv_trades_parse_correct_row_count(self, app):
        from app.services.ingestion import ingest_auto
        csv = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-02-01,ACCT99,AAPL,10,190.00,BUY,2025-02-03\n"
            "2025-02-01,ACCT99,MSFT,5,430.00,BUY,2025-02-03\n"
        )
        with app.app_context():
            result = ingest_auto(csv)
        assert result["rows_inserted"] == 2
        assert result["quality_issues"] == []

    def test_pipe_delimited_derives_trade_type_for_sell(self, app):
        from app.services.ingestion import ingest_auto
        from app.models import Trade
        pipe = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250202|ACCT88|TSLA|-50|-12000.00|CUSTODIAN_X\n"
        )
        with app.app_context():
            result = ingest_auto(pipe)
            trade = Trade.query.filter_by(account_id="ACCT88", ticker="TSLA").first()
            assert trade is not None
            assert trade.trade_type == "SELL"

    def test_positions_yaml_parses_report_date(self, app):
        from app.services.ingestion import ingest_auto
        from app.models import Position
        from datetime import date
        yaml_str = (
            "report_date: '20250301'\n"
            "positions:\n"
            "  - account_id: 'ACCT77'\n"
            "    ticker: 'GOOG'\n"
            "    shares: 10\n"
            "    market_value: 1500.00\n"
        )
        with app.app_context():
            result = ingest_auto(yaml_str)
            pos = Position.query.filter_by(account_id="ACCT77", ticker="GOOG").first()
            assert pos is not None
            assert pos.report_date == date(2025, 3, 1)

    def test_positions_yaml_missing_report_date(self, app):
        from app.services.ingestion import ingest_auto
        yaml_str = "positions:\n  - account_id: 'X'\n    ticker: 'Y'\n    shares: 1\n    market_value: 100\n"
        with app.app_context():
            result = ingest_auto(yaml_str)
        assert len(result["quality_issues"]) > 0
        assert result["rows_inserted"] == 0
