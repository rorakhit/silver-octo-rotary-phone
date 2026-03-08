"""Tests for GET /compliance/concentration endpoint."""


class TestComplianceEndpoint:
    def test_compliance_requires_date(self, client):
        resp = client.get("/compliance/concentration")
        assert resp.status_code == 400

    def test_compliance_invalid_date(self, client):
        resp = client.get("/compliance/concentration?date=01-15-2025")
        assert resp.status_code == 400

    def test_compliance_returns_threshold(self, client):
        resp = client.get("/compliance/concentration?date=2025-01-15")
        data = resp.get_json()
        assert data["threshold_pct"] == 20.0

    def test_compliance_no_data_returns_empty(self, client):
        resp = client.get("/compliance/concentration?date=1999-01-01")
        data = resp.get_json()
        assert data["violations"] == []

    def test_compliance_detects_violation(self, client):
        """
        ACC002 positions: AAPL $37100, NVDA $60636, TSLA $19076 => total $116812
        NVDA concentration: 60636/116812 = 51.9% => violation
        """
        resp = client.get("/compliance/concentration?date=2025-01-15")
        assert resp.status_code == 200
        data = resp.get_json()
        tickers_in_violation = {
            (v["account_id"], v["ticker"]) for v in data["violations"]
        }
        assert ("ACC002", "NVDA") in tickers_in_violation

    def test_compliance_violation_has_required_fields(self, client):
        resp = client.get("/compliance/concentration?date=2025-01-15")
        data = resp.get_json()
        assert len(data["violations"]) > 0
        violation = data["violations"][0]
        for field in ("account_id", "ticker", "concentration_pct", "excess_pct",
                      "position_market_value", "account_total_market_value"):
            assert field in violation

    def test_compliance_concentration_pct_correct(self, client):
        resp = client.get("/compliance/concentration?date=2025-01-15")
        data = resp.get_json()
        nvda_acc002 = next(
            (v for v in data["violations"] if v["account_id"] == "ACC002" and v["ticker"] == "NVDA"),
            None
        )
        assert nvda_acc002 is not None
        total = 37100.00 + 60636.00 + 19076.00
        expected_pct = round(60636.00 / total * 100, 2)
        assert abs(nvda_acc002["concentration_pct"] - expected_pct) < 0.01
