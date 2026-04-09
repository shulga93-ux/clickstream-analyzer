import os
import uuid
from pathlib import Path
from flask import Flask, request, render_template, send_file, jsonify

from analyzer.parser import parse_file, get_summary
from analyzer.detector import detect_all
from analyzer.reporter import generate_report

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB

UPLOAD_DIR = Path("uploads")
REPORT_DIR = Path("reports")
UPLOAD_DIR.mkdir(exist_ok=True)
REPORT_DIR.mkdir(exist_ok=True)

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}

METRIC_LABELS = {
    55556: "Тех Ошибка",
    55557: "Тех Ошибка (UnknownApp)",
    55558: "Статусный экран",
}


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
        return jsonify({"error": "Unsupported format. Use: CSV, XLSX"}), 400

    suffix = Path(file.filename).suffix.lower()
    upload_id = str(uuid.uuid4())[:8]
    upload_path = UPLOAD_DIR / f"{upload_id}{suffix}"
    file.save(upload_path)

    try:
        df_full = parse_file(str(upload_path))

        reports = []
        total_anomalies = 0
        total_wow = 0
        total_dod = 0

        # Generate one report per metric_id
        for metric_id, label in METRIC_LABELS.items():
            df_m = df_full[df_full["metric_id"] == metric_id].copy()
            if df_m.empty:
                continue

            summary = get_summary(df_m)
            results = detect_all(df_m)

            slug = str(metric_id)
            report_filename = f"report_{upload_id}_{slug}.html"
            report_path = REPORT_DIR / report_filename
            generate_report(df_m, summary, results, str(report_path))

            total_anomalies += len(results.get("anomalies", []))
            total_wow += len(results.get("wow", []))
            total_dod += len(results.get("dod", []))

            reports.append({
                "metric_id": metric_id,
                "label": label,
                "report_url": f"/reports/{report_filename}",
                "total_records": summary.get("total_records", 0),
                "total_val": summary.get("total_val", 0),
                "unique_products": summary.get("unique_products", 0),
                "date_range": summary.get("date_range", {}),
                "anomalies_found": len(results.get("anomalies", [])),
                "wow_deviations": len(results.get("wow", [])),
                "dod_deviations": len(results.get("dod", [])),
            })

        if not reports:
            return jsonify({"error": "Нет данных ни по одной метрике"}), 422

        # Also keep a combined summary
        summary_full = get_summary(df_full)

        return jsonify({
            "status": "ok",
            "reports": reports,
            # backward-compat: first report url
            "report_url": reports[0]["report_url"],
            "summary": {
                "total_records": summary_full.get("total_records", 0),
                "unique_products": summary_full.get("unique_products", 0),
                "date_range": summary_full.get("date_range", {}),
                "anomalies_found": total_anomalies,
                "wow_deviations": total_wow,
                "dod_deviations": total_dod,
            }
        })

    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        app.logger.exception("Analysis failed")
        return jsonify({"error": f"Analysis failed: {str(e)}"}), 500
    finally:
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
    items = [{"name": p.name, "url": f"/reports/{p.name}"} for p in reports[:30]]
    return jsonify(items)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
