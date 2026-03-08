"""
POST /ingest

Accepts multipart form-data with one or more files under any key name.
All files are auto-detected: the system sniffs the format, delimiter, and
column headers automatically.

Returns a JSON data-quality report for each file provided.
"""

from flask import Blueprint, jsonify, request

from app.services.ingestion import ingest_auto

ingest_bp = Blueprint("ingest", __name__)


@ingest_bp.route("/ingest", methods=["POST"])
def ingest():
    reports = []
    errors = []

    if not request.files:
        return jsonify({
            "error": "No files provided. Upload one or more files under any key name."
        }), 400

    # Process every uploaded file regardless of its form key
    for key in request.files:
        for file in request.files.getlist(key):
            if file.filename == "":
                errors.append(f"Empty filename for key '{key}'")
                continue
            try:
                content = file.read().decode("utf-8")
                report = ingest_auto(content)
                report["file"] = file.filename
                reports.append(report)
            except Exception as exc:
                errors.append(f"Failed to process '{file.filename}': {exc}")

    status = "ok" if not errors else "partial"
    return jsonify({"status": status, "reports": reports, "errors": errors}), 200
