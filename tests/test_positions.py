"""Tests for GET /positions endpoint."""


class TestPositionsEndpoint:
    def test_positions_requires_account_param(self, client):
        resp = client.get("/positions?date=2025-01-15")
        assert resp.status_code == 400

    def test_positions_requires_date_param(self, client):
        resp = client.get("/positions?account=ACC001")
        assert resp.status_code == 400

    def test_positions_invalid_date_format(self, client):
        resp = client.get("/positions?account=ACC001&date=20250115")
        assert resp.status_code == 400

    def test_positions_known_account_returns_data(self, client):
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["account"] == "ACC001"
        assert data["date"] == "2025-01-15"
        assert len(data["positions"]) == 3

    def test_positions_unknown_account_returns_empty(self, client):
        resp = client.get("/positions?account=UNKNOWN&date=2025-01-15")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["positions"] == []

    def test_positions_includes_cost_basis_from_format1(self, client):
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        data = resp.get_json()
        aapl = next(p for p in data["positions"] if p["ticker"] == "AAPL")
        # 100 shares * $185.50 = $18,550
        assert aapl["cost_basis"] == 18550.0

    def test_positions_includes_market_value(self, client):
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        data = resp.get_json()
        msft = next(p for p in data["positions"] if p["ticker"] == "MSFT")
        assert msft["market_value"] == 21012.50

    def test_positions_total_market_value(self, client):
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        data = resp.get_json()
        expected = 18550.00 + 21012.50 + 10710.00
        assert abs(data["total_market_value"] - expected) < 0.01
