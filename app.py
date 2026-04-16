import os
import shutil
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

    # Check free disk space (need at least 50 MB)
    disk = shutil.disk_usage(REPORT_DIR)
    if disk.free < 50 * 1024 * 1024:
        free_mb = disk.free // (1024 * 1024)
        return jsonify({"error": f"Недостаточно места на диске: свободно {free_mb} МБ, нужно минимум 50 МБ. Удалите старые файлы."}), 507

    # Cleanup old reports — keep only last 5 report sets (each set = 4 files: svod+55556+55557+55558)
    all_reports = sorted(REPORT_DIR.glob("*.html"), key=lambda p: p.stat().st_mtime)
    KEEP_REPORTS = 20  # keep last 20 files (~5 full sets of 4 reports each)
    if len(all_reports) > KEEP_REPORTS:
        for old in all_reports[:len(all_reports) - KEEP_REPORTS]:
            try:
                old.unlink()
            except Exception:
                pass

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
        all_results_for_svod = []

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
                df_success = df_m[df_m["lvl_4"] == "Успех"].copy()
                pd_error = detect_product_dynamics(df_error)   # stats on Ошибка only
                # Build success matrix for ratio line: use same product list as pd_error
                error_products = [pm["name"] for pm in pd_error.get("products", [])]
                pd_error["success_matrix"] = {}
                if not df_success.empty and error_products:
                    import pandas as _pd2
                    df_s2 = df_success[df_success["lvl_2"].isin(error_products)].copy()
                    df_s2["date"] = df_s2["report_dt"].dt.date.astype(str)
                    suc_grp = df_s2.groupby(["lvl_2", "date"])["val"].sum()
                    for (prod, date), val in suc_grp.items():
                        if prod not in pd_error["success_matrix"]:
                            pd_error["success_matrix"][prod] = {}
                        pd_error["success_matrix"][prod][date] = int(val)
                # lvl4_matrix still needed for use_lvl4 detection; build with large top_n
                pd_full = detect_product_dynamics(df_m, top_n=200)
                pd_error["lvl4_matrix"] = pd_full["lvl4_matrix"]

                # Compute error/success ratio and trend per product
                import datetime as _dta
                err_suc_lookup = {}
                for _pm in pd_error.get("products", []):
                    _p = _pm["name"]
                    _em = pd_error.get("matrix", {}).get(_p, {})
                    _sm = pd_error.get("success_matrix", {}).get(_p, {})
                    # Overall ratio (%)
                    _te = sum(_em.values())
                    _ts = sum(_sm.values())
                    _ratio = round(_te / (_te + _ts) * 100, 1) if (_te + _ts) > 0 else None
                    _pm["err_suc_ratio"] = _ratio
                    # Trend: compare last 7 non-Sunday days ratio vs prev 7
                    _dns = sorted([d for d in _em if _dta.date.fromisoformat(d).weekday() != 6])
                    _tpct, _tdir = None, "stable"
                    if len(_dns) >= 4:
                        _h = min(7, len(_dns) // 2)
                        _cd = _dns[-_h:]; _pd_ = _dns[-2*_h:-_h]
                        _ce = sum(_em.get(d,0) for d in _cd); _cs = sum(_sm.get(d,0) for d in _cd)
                        _pe = sum(_em.get(d,0) for d in _pd_); _ps = sum(_sm.get(d,0) for d in _pd_)
                        _cr = _ce / (_ce + _cs) * 100 if (_ce + _cs) > 0 else None
                        _pr = _pe / (_pe + _ps) * 100 if (_pe + _ps) > 0 else None
                        if _cr is not None and _pr and _pr > 0:
                            _tpct = round((_cr - _pr) / _pr * 100, 1)
                            _tdir = "growing" if _tpct > 15 else ("declining" if _tpct < -15 else "stable")
                    _pm["err_suc_trend_pct"] = _tpct
                    _pm["err_suc_trend_dir"] = _tdir
                    err_suc_lookup[_p] = {"ratio": _ratio, "trend_pct": _tpct, "trend_dir": _tdir}
                pd_error["err_suc_lookup"] = err_suc_lookup

                results["product_dynamics"] = pd_error

            slug = str(metric_id)
            report_filename = f"report_{upload_id}_{slug}.html"
            report_path = REPORT_DIR / report_filename
            generate_report(df_m, summary, results, str(report_path), metric_id=metric_id)

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
