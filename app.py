import os
import uuid
from pathlib import Path
from flask import Flask, request, render_template, send_file, jsonify, redirect, url_for

from analyzer.parser import parse_file, get_summary
from analyzer.detector import detect_all
from analyzer.reporter import generate_report

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB

UPLOAD_DIR = Path("uploads")
REPORT_DIR = Path("reports")
UPLOAD_DIR.mkdir(exist_ok=True)
REPORT_DIR.mkdir(exist_ok=True)

ALLOWED_EXTENSIONS = {".csv", ".json", ".jsonl", ".ndjson"}


def allowed_file(filename: str) -> bool:
    return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    if not file.filename:
        return jsonify({"error": "Empty filename"}), 400

    if not allowed_file(file.filename):
        return jsonify({"error": f"Unsupported format. Use: CSV, JSON, JSONL"}), 400

    # Save upload
    suffix = Path(file.filename).suffix.lower()
    upload_id = str(uuid.uuid4())[:8]
    upload_path = UPLOAD_DIR / f"{upload_id}{suffix}"
    file.save(upload_path)

    try:
        # Parse
        df = parse_file(str(upload_path))
        summary = get_summary(df)

        # Analyze
        results = detect_all(df)

        # Generate report
        report_filename = f"report_{upload_id}.html"
        report_path = REPORT_DIR / report_filename
        generate_report(df, summary, results, str(report_path))

        return jsonify({
            "status": "ok",
            "report_url": f"/reports/{report_filename}",
            "summary": {
                "total_events": summary["total_events"],
                "unique_users": summary["unique_users"],
                "unique_sessions": summary["unique_sessions"],
                "anomalies_found": len(results.get("anomalies", [])),
                "deviations_found": len(results.get("deviations", [])),
            }
        })

    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        app.logger.exception("Analysis failed")
        return jsonify({"error": f"Analysis failed: {str(e)}"}), 500
    finally:
        # Clean up upload
        try:
            upload_path.unlink()
        except Exception:
            pass


@app.route("/reports/<filename>")
def get_report(filename):
    report_path = REPORT_DIR / filename
    if not report_path.exists():
        return "Report not found", 404
    return send_file(str(report_path))


@app.route("/reports")
def list_reports():
    reports = sorted(REPORT_DIR.glob("*.html"), key=lambda p: p.stat().st_mtime, reverse=True)
    items = [{"name": p.name, "url": f"/reports/{p.name}"} for p in reports[:20]]
    return jsonify(items)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
