from datetime import datetime, timezone
from app import db


class Trade(db.Model):
    """
    Unified trades table. Stores trade data from any source/format.

    All files are auto-detected and ingested through a single code path.
    The source_system field identifies where the data originated (e.g.
    'CUSTODIAN_A', 'BLOOMBERG') — defaults to 'internal' when no source
    is specified in the file.
    """

    __tablename__ = "trades"

    id = db.Column(db.Integer, primary_key=True)
    trade_date = db.Column(db.Date, nullable=False, index=True)
    account_id = db.Column(db.String(50), nullable=False, index=True)
    ticker = db.Column(db.String(20), nullable=False, index=True)
    quantity = db.Column(db.Float, nullable=False)
    price = db.Column(db.Float, nullable=True)
    trade_type = db.Column(db.String(10), nullable=True)  # BUY/SELL
    settlement_date = db.Column(db.Date, nullable=True)
    market_value = db.Column(db.Float, nullable=True)
    source_system = db.Column(db.String(50), nullable=False, default="internal")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint(
            "trade_date", "account_id", "ticker", "quantity", "source_system",
            name="uq_trade"
        ),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "trade_date": self.trade_date.isoformat(),
            "account_id": self.account_id,
            "ticker": self.ticker,
            "quantity": self.quantity,
            "price": self.price,
            "trade_type": self.trade_type,
            "settlement_date": self.settlement_date.isoformat() if self.settlement_date else None,
            "market_value": self.market_value,
            "source_system": self.source_system,
        }


class Position(db.Model):
    """
    Bank/broker positions. Represents the official end-of-day
    position snapshot from a custodian.
    """

    __tablename__ = "positions"

    id = db.Column(db.Integer, primary_key=True)
    report_date = db.Column(db.Date, nullable=False, index=True)
    account_id = db.Column(db.String(50), nullable=False, index=True)
    ticker = db.Column(db.String(20), nullable=False, index=True)
    shares = db.Column(db.Float, nullable=False)
    market_value = db.Column(db.Float, nullable=False)
    custodian_ref = db.Column(db.String(50), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint(
            "report_date", "account_id", "ticker",
            name="uq_position"
        ),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "report_date": self.report_date.isoformat(),
            "account_id": self.account_id,
            "ticker": self.ticker,
            "shares": self.shares,
            "market_value": self.market_value,
            "custodian_ref": self.custodian_ref,
        }
