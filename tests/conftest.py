import io
import pytest

from app import create_app, db as _db


INTERNAL_TRADES_CSV = """\
TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate
2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17
2025-01-15,ACC001,MSFT,50,420.25,BUY,2025-01-17
2025-01-15,ACC002,GOOGL,75,142.80,BUY,2025-01-17
2025-01-15,ACC002,AAPL,200,185.50,BUY,2025-01-17
2025-01-15,ACC003,TSLA,150,238.45,SELL,2025-01-17
2025-01-15,ACC003,NVDA,80,505.30,BUY,2025-01-17
2025-01-15,ACC001,GOOGL,100,142.80,BUY,2025-01-17
2025-01-15,ACC004,AAPL,500,185.50,BUY,2025-01-17
2025-01-15,ACC004,MSFT,300,420.25,BUY,2025-01-17
2025-01-15,ACC002,NVDA,120,505.30,BUY,2025-01-17
"""

CUSTODIAN_TRADES_CSV = """\
REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM
20250115|ACC001|AAPL|100|18550.00|CUSTODIAN_A
20250115|ACC001|MSFT|50|21012.50|CUSTODIAN_A
20250115|ACC001|GOOGL|100|14280.00|CUSTODIAN_A
20250115|ACC002|GOOGL|75|10710.00|CUSTODIAN_B
20250115|ACC002|AAPL|200|37100.00|CUSTODIAN_B
20250115|ACC002|NVDA|120|60636.00|CUSTODIAN_B
20250115|ACC003|TSLA|-150|-35767.50|CUSTODIAN_A
20250115|ACC003|NVDA|80|40424.00|CUSTODIAN_A
20250115|ACC004|AAPL|500|92750.00|CUSTODIAN_C
20250115|ACC004|MSFT|300|126075.00|CUSTODIAN_C
"""

POSITIONS_YAML = """\
report_date: "20250115"
positions:
  - account_id: "ACC001"
    ticker: "AAPL"
    shares: 100
    market_value: 18550.00
    custodian_ref: "CUST_A_12345"
  - account_id: "ACC001"
    ticker: "MSFT"
    shares: 50
    market_value: 21012.50
    custodian_ref: "CUST_A_12346"
  - account_id: "ACC001"
    ticker: "GOOGL"
    shares: 75
    market_value: 10710.00
    custodian_ref: "CUST_A_12347"
  - account_id: "ACC002"
    ticker: "AAPL"
    shares: 200
    market_value: 37100.00
    custodian_ref: "CUST_B_22345"
  - account_id: "ACC002"
    ticker: "NVDA"
    shares: 120
    market_value: 60636.00
    custodian_ref: "CUST_B_22346"
  - account_id: "ACC002"
    ticker: "TSLA"
    shares: 80
    market_value: 19076.00
    custodian_ref: "CUST_B_22347"
  - account_id: "ACC003"
    ticker: "MSFT"
    shares: 150
    market_value: 63037.50
    custodian_ref: "CUST_A_32345"
  - account_id: "ACC003"
    ticker: "GOOGL"
    shares: 100
    market_value: 14280.00
    custodian_ref: "CUST_A_32346"
  - account_id: "ACC003"
    ticker: "AAPL"
    shares: 50
    market_value: 9275.00
    custodian_ref: "CUST_A_32347"
"""


@pytest.fixture(scope="session")
def app():
    app = create_app("testing")
    return app


@pytest.fixture(scope="session")
def db(app):
    with app.app_context():
        _db.create_all()
        yield _db
        _db.drop_all()


@pytest.fixture(scope="session")
def client(app, db):
    return app.test_client()


@pytest.fixture(scope="session", autouse=True)
def seed_data(client):
    """Ingest all three sample files once for the whole test session."""
    data = {
        "internal_trades": (io.BytesIO(INTERNAL_TRADES_CSV.encode()), "internal_trades.csv"),
        "custodian_trades": (io.BytesIO(CUSTODIAN_TRADES_CSV.encode()), "custodian_trades.csv"),
        "bank_positions": (io.BytesIO(POSITIONS_YAML.encode()), "positions.yaml"),
    }
    resp = client.post("/ingest", data=data, content_type="multipart/form-data")
    assert resp.status_code == 200
