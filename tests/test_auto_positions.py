"""Tests for auto-detection of position files (structured and tabular)."""

import io
import json


class TestStructuredPositionAutoDetect:
    """YAML/JSON position files with non-standard field names."""

    def test_yaml_with_alternate_keys(self, client):
        """as_of_date + holdings list with sym/qty/mv keys."""
        yaml_str = (
            "as_of_date: '2025-10-01'\n"
            "holdings:\n"
            "  - acct: 'POS_Y1'\n"
            "    symbol: 'AAPL'\n"
            "    units: 200\n"
            "    mkt_value: 38000.00\n"
            "    cust_ref: 'REF_001'\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(yaml_str.encode()), "holdings.yaml"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["format"] == "positions"
        assert report["rows_inserted"] == 1

    def test_json_with_alternate_keys(self, client):
        payload = {
            "snapshot_date": "2025-10-02",
            "positions": [
                {"portfolio": "POS_J1", "security": "TSLA", "shares": 50, "total_value": 12000.0},
                {"portfolio": "POS_J1", "security": "NVDA", "shares": 30, "total_value": 15000.0},
            ]
        }
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(json.dumps(payload).encode()), "snap.json"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["format"] == "positions"
        assert report["rows_inserted"] == 2

    def test_yaml_missing_date_key_fails_gracefully(self, client):
        yaml_str = (
            "holdings:\n"
            "  - acct: 'X'\n"
            "    symbol: 'Y'\n"
            "    shares: 1\n"
            "    market_value: 100\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(yaml_str.encode()), "nodate.yaml"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["rows_inserted"] == 0
        assert any("date" in q.lower() for q in report["quality_issues"])

    def test_yaml_unmappable_fields_reported(self, client):
        yaml_str = (
            "report_date: '20251003'\n"
            "positions:\n"
            "  - account_id: 'POS_UNK'\n"
            "    ticker: 'META'\n"
            "    shares: 10\n"
            "    market_value: 5000\n"
            "    weirdfield: 'hello'\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(yaml_str.encode()), "unk.yaml"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["rows_inserted"] == 1
        unmapped_issues = [q for q in report["quality_issues"] if "Unmapped" in q]
        assert len(unmapped_issues) > 0


class TestTabularPositionAutoDetect:
    """CSV/pipe/tab position files should route to the positions table."""

    def test_csv_positions_with_custodian_ref(self, client):
        """File with custodian_ref column → auto-classified as positions."""
        csv = (
            "ReportDate,Account,Ticker,Shares,MarketValue,CustodianRef\n"
            "2025-11-01,POS_T1,AAPL,100,19000.00,CUST_99\n"
            "2025-11-01,POS_T1,MSFT,50,22000.00,CUST_100\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(csv.encode()), "pos_tabular.csv"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["format"] == "auto_positions"
        assert report["rows_inserted"] == 2

    def test_pipe_positions_with_broker_ref(self, client):
        pipe = (
            "Date|AcctNo|Symbol|Holdings|MktValue|BrokerRef\n"
            "20251201|POS_P1|GOOGL|75|11000.00|BRK_001\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(pipe.encode()), "broker_pos.dat"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["format"] == "auto_positions"
        assert report["rows_inserted"] == 1

    def test_tabular_with_trade_fields_routes_to_trades(self, client):
        """File with price + trade_type → classified as trades, NOT positions."""
        csv = (
            "Date,Account,Ticker,Qty,Price,Side\n"
            "2025-12-01,TRD_CLS,AAPL,50,200.00,BUY\n"
        )
        resp = client.post("/ingest", data={
            "file": (io.BytesIO(csv.encode()), "trades_check.csv"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        assert report["format"] == "auto_tabular"  # trades, not positions


class TestPositionsUnderAnyKey:
    """YAML positions should be auto-detected regardless of form key name."""

    def test_positions_under_arbitrary_key(self, client):
        yaml_str = (
            "report_date: '20251301'\n"  # intentionally bad date
            "positions:\n"
            "  - account_id: 'X'\n"
            "    ticker: 'Y'\n"
            "    shares: 1\n"
            "    market_value: 100\n"
        )
        resp = client.post("/ingest", data={
            "bank_data": (io.BytesIO(yaml_str.encode()), "pos.yaml"),
        }, content_type="multipart/form-data")
        data = resp.get_json()
        report = data["reports"][0]
        # Should be auto-detected as positions regardless of key name
        assert report["format"] == "positions"
