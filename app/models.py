from datetime import date, datetime, timezone

from sqlalchemy import func

from app import db


class Trade(db.Model):
    """
    Unified trades table. Stores trade data from any source/format.

    All files are auto-detected and ingested through a single code path.
    The source_system field identifies where the data originated (e.g.
    'CUSTODIAN_A', 'BLOOMBERG') — defaults to 'internal' when no source
    is specified in the file.

    Nullable fields (price, trade_type, settlement_date, market_value)
    reflect real-world data variability: internal files typically include
    price but not market_value, while custodian files often provide
    market_value but omit per-share price.
    """

    __tablename__ = "trades"

    id = db.Column(db.Integer, primary_key=True)
    trade_date = db.Column(db.Date, nullable=False, index=True)
    account_id = db.Column(db.String(50), nullable=False, index=True)
    ticker = db.Column(db.String(20), nullable=False, index=True)
    quantity = db.Column(db.Float, nullable=False)
    price = db.Column(db.Float, nullable=True)           # per-share price; nullable for custodian files
    trade_type = db.Column(db.String(10), nullable=True)  # BUY/SELL; inferred from quantity sign if absent
    settlement_date = db.Column(db.Date, nullable=True)
    market_value = db.Column(db.Float, nullable=True)     # notional value; nullable for internal files
    source_system = db.Column(db.String(50), nullable=False, default="internal")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    # Dedup key: same trade from the same source won't be inserted twice.
    # Including source_system lets the same trade exist once per source
    # (e.g. internal + custodian), which is needed for reconciliation.
    __table_args__ = (
        db.UniqueConstraint(
            "trade_date", "account_id", "ticker", "quantity", "source_system",
            name="uq_trade"
        ),
    )

    # ----- Query helpers -----

    @classmethod
    def exists(cls, trade_date, account_id, ticker, quantity, source_system):
        """Check if a trade matching the unique constraint already exists."""
        return cls.query.filter_by(
            trade_date=trade_date,
            account_id=account_id,
            ticker=ticker,
            quantity=quantity,
            source_system=source_system,
        ).first()

    @classmethod
    def cost_basis_by_ticker(cls, account_id: str, as_of_date: date) -> dict[str, float]:
        """
        Cumulative cost basis per ticker for an account up to a given date.

        Only includes trades with a per-share price (custodian trades that
        report market_value but omit price are naturally excluded).

        Returns:
            {ticker: gross_cost} rounded to 2 decimal places
        """
        rows = (
            cls.query
            .with_entities(
                cls.ticker,
                func.sum(cls.quantity * cls.price).label("gross_cost"),
            )
            .filter(
                cls.account_id == account_id,
                cls.trade_date <= as_of_date,
                cls.price.isnot(None),
            )
            .group_by(cls.ticker)
            .all()
        )
        return {row.ticker: round(row.gross_cost, 2) for row in rows}

    @classmethod
    def custodian_positions(cls, trade_date: date) -> dict[tuple, dict]:
        """
        Aggregate custodian-sourced trades by (account, ticker) for a date.

        Custodian trades are identified by source_system != 'internal'.

        Returns:
            {(account_id, ticker): {"shares": total, "market_value": total}}
        """
        rows = (
            cls.query
            .with_entities(
                cls.account_id,
                cls.ticker,
                func.sum(cls.quantity).label("total_shares"),
                func.sum(cls.market_value).label("total_mv"),
            )
            .filter(
                cls.trade_date == trade_date,
                cls.source_system != "internal",
            )
            .group_by(cls.account_id, cls.ticker)
            .all()
        )
        return {
            (r.account_id, r.ticker): {"shares": r.total_shares, "market_value": r.total_mv}
            for r in rows
        }

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

    These are the "source of truth" positions used by the compliance
    and reconciliation endpoints. Each row is one security in one
    account on one date.
    """

    __tablename__ = "positions"

    id = db.Column(db.Integer, primary_key=True)
    report_date = db.Column(db.Date, nullable=False, index=True)
    account_id = db.Column(db.String(50), nullable=False, index=True)
    ticker = db.Column(db.String(20), nullable=False, index=True)
    shares = db.Column(db.Float, nullable=False)
    market_value = db.Column(db.Float, nullable=False)
    custodian_ref = db.Column(db.String(50), nullable=True)  # optional custodian reference ID
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    # One position per security per account per day
    __table_args__ = (
        db.UniqueConstraint(
            "report_date", "account_id", "ticker",
            name="uq_position"
        ),
    )

    # ----- Query helpers -----

    @classmethod
    def for_account_and_date(cls, account_id: str, report_date: date):
        """All positions for a specific account on a specific date."""
        return cls.query.filter_by(
            report_date=report_date, account_id=account_id
        ).all()

    @classmethod
    def for_date(cls, report_date: date):
        """All positions for a given date across all accounts."""
        return cls.query.filter_by(report_date=report_date).all()

    @classmethod
    def exists(cls, report_date, account_id, ticker):
        """Check if a position matching the unique constraint already exists."""
        return cls.query.filter_by(
            report_date=report_date,
            account_id=account_id,
            ticker=ticker,
        ).first()

    @classmethod
    def account_totals(cls, report_date: date) -> dict[str, float]:
        """
        Total market value per account for a given date.

        Returns:
            {account_id: total_market_value}
        """
        rows = (
            cls.query
            .with_entities(
                cls.account_id,
                func.sum(cls.market_value).label("total_mv"),
            )
            .filter_by(report_date=report_date)
            .group_by(cls.account_id)
            .all()
        )
        return {row.account_id: row.total_mv for row in rows}

    @classmethod
    def as_lookup(cls, report_date: date) -> dict[tuple, dict]:
        """
        All positions for a date as a lookup dict keyed by (account, ticker).

        Returns:
            {(account_id, ticker): {"shares": ..., "market_value": ...}}
        """
        rows = cls.for_date(report_date)
        return {
            (p.account_id, p.ticker): {"shares": p.shares, "market_value": p.market_value}
            for p in rows
        }

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
