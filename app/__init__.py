from flask import Flask
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


def create_app(config_name="default"):
    from config import config

    app = Flask(__name__)
    app.config.from_object(config[config_name])

    db.init_app(app)

    from app.routes.ingest import ingest_bp
    from app.routes.positions import positions_bp
    from app.routes.compliance import compliance_bp
    from app.routes.reconciliation import reconciliation_bp

    app.register_blueprint(ingest_bp)
    app.register_blueprint(positions_bp)
    app.register_blueprint(compliance_bp)
    app.register_blueprint(reconciliation_bp)

    with app.app_context():
        db.create_all()

    return app
