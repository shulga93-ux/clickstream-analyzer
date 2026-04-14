import os
import uuid
from pathlib import Path
from functools import wraps
from flask import Flask, request, render_template, send_file, jsonify, session, redirect, url_for
from werkzeug.security import check_password_hash

# Load .env if present
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

from analyzer.parser import parse_file, get_summary
from analyzer.detector import detect_all
from analyzer.reporter import generate_report
from analyzer.candidates import generate_candidates_report

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")

AUTH_USER = os.environ.get("AUTH_USER", "admin")
AUTH_PASSWORD_HASH = os.environ.get("AUTH_PASSWORD_HASH", "")


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return decorated

UPLOAD_DIR = Path("uploads")
REPORT_DIR = Path("reports")
UPLOAD_DIR.mkdir(exist_ok=True)
REPORT_DIR.mkdir(exist_ok=True)

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}

METRIC_LABELS = {
    "55556": "Тех Ошибка",
    "55557": "Тех Ошибка (UnknownApp)",
    "55558": "Статусный экран",
}


def allowed_file(filename: str) -> bool:
    return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS


@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("logged_in"):
        return redirect(url_for("index"))
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if username == AUTH_USER and AUTH_PASSWORD_HASH and check_password_hash(AUTH_PASSWORD_HASH, password):
            session["logged_in"] = True
            next_url = request.args.get("next") or url_for("index")
            return redirect(next_url)
        error = "Неверный логин или пароль"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
@login_required
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

            # For Статусный экран: timeline/WoW/DoD only on lvl_4=="Ошибка"
            # but product_dynamics uses full df_m (all lvl_4 types)
            if metric_id == "55558":
                df_analysis = df_m[df_m["lvl_4"] == "Ошибка"].copy()
            else:
                df_analysis = df_m

            summary = get_summary(df_analysis)
            results = detect_all(df_analysis)

            # Override product_dynamics for 55558:
            # - products/total_val/DoD/WoW/matrix → only lvl_4="Ошибка"
            # - lvl4_matrix (for chart breakdown) → all lvl_4 types
            if metric_id == "55558":
                from analyzer.detector import detect_product_dynamics
                df_error = df_m[df_m["lvl_4"] == "Ошибка"].copy()
                pd_error = detect_product_dynamics(df_error)   # stats on Ошибка only
                pd_full  = detect_product_dynamics(df_m)       # lvl4_matrix for chart
                pd_error["lvl4_matrix"] = pd_full["lvl4_matrix"]   # inject full lvl4 breakdown
                results["product_dynamics"] = pd_error

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

        # Generate "Кандидаты в Проблемы" report (across all metrics)
        candidates_filename = f"report_{upload_id}_candidates.html"
        candidates_path = REPORT_DIR / candidates_filename
        generate_candidates_report(df_full, str(candidates_path))
        reports.append({
            "metric_id": "candidates",
            "label": "Кандидаты в Проблемы",
            "report_url": f"/reports/{candidates_filename}",
            "total_records": len(df_full),
            "total_val": int(df_full["val"].sum()),
            "unique_products": int(df_full["lvl_2"].nunique()),
            "date_range": {},
            "anomalies_found": 0,
            "wow_deviations": 0,
            "dod_deviations": 0,
        })

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
@login_required
def get_report(filename):
    report_path = REPORT_DIR / filename
    if not report_path.exists():
        return "Report not found", 404
    return send_file(str(report_path))


@app.route("/reports")
@login_required
def list_reports():
    reports = sorted(REPORT_DIR.glob("*.html"), key=lambda p: p.stat().st_mtime, reverse=True)
    items = [{"name": p.name, "url": f"/reports/{p.name}"} for p in reports[:30]]
    return jsonify(items)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
