"""
Кандидаты report: products where ratio 55556_errors/(55556_errors+55558_success)
is growing week-over-week. Format similar to Статусный Экран (55558).
"""
from __future__ import annotations
import datetime as _dt
import json
from pathlib import Path as _Path
import pandas as pd


# ─── helpers ──────────────────────────────────────────────────────────────────

def _no_sunday(d: _dt.date) -> bool:
    return d.weekday() != 6


def _ratio(err: float, suc: float) -> float | None:
    total = err + suc
    return round(err / total * 100, 2) if total > 0 else None


def _wow_trend(series: dict[str, float], dates_no_sun: list[str]):
    """Compare avg ratio last 7 days vs prev 7 days. Returns (pct_change, direction)."""
    avail = [d for d in dates_no_sun if d in series]
    if len(avail) < 4:
        return None, "stable"
    h = min(7, len(avail) // 2)
    curr_vals = [series[d] for d in avail[-h:]]
    prev_vals = [series[d] for d in avail[-2*h:-h]]
    curr_avg = sum(curr_vals) / len(curr_vals)
    prev_avg = sum(prev_vals) / len(prev_vals)
    if prev_avg == 0:
        return None, "stable"
    pct = round((curr_avg - prev_avg) / prev_avg * 100, 1)
    direction = "growing" if pct > 15 else ("declining" if pct < -15 else "stable")
    return pct, direction


# ─── main entry point ─────────────────────────────────────────────────────────

def generate_candidates_report(df_full: pd.DataFrame, output_path: str) -> int:
    """
    Build Кандидаты report: products with growing error/success ratio (55556/55558).
    Returns count of candidates.
    """
    df_56 = df_full[df_full["metric_id"] == "55556"].copy()
    df_58 = df_full[df_full["metric_id"] == "55558"].copy()

    if df_56.empty or df_58.empty:
        _Path(output_path).write_text(_empty_html(), encoding="utf-8")
        return 0

    # Daily errors per product from 55556
    df_56["date"] = df_56["report_dt"].dt.date
    err_grp = df_56.groupby(["lvl_2", "lvl_1", "date"])["val"].sum().reset_index()
    err_grp.columns = ["product", "segment", "date", "errors"]

    # Daily successes per product from 55558
    df_58["date"] = df_58["report_dt"].dt.date
    suc_58 = df_58[df_58["lvl_4"] == "Успех"].copy()
    suc_grp = suc_58.groupby(["lvl_2", "date"])["val"].sum().reset_index()
    suc_grp.columns = ["product", "date", "success"]

    # Merge on (product, date)
    merged = err_grp.merge(suc_grp, on=["product", "date"], how="left")
    merged["success"] = merged["success"].fillna(0)
    merged["date_str"] = merged["date"].astype(str)

    # All non-Sunday dates sorted
    all_dates = sorted(merged["date"].unique())
    dates_no_sun = sorted([str(d) for d in all_dates if _no_sunday(d)])

    # Per-product ratio series + trend
    products_data = []
    for (prod, seg), grp in merged.groupby(["product", "segment"]):
        series: dict[str, float] = {}
        for _, row in grp.iterrows():
            r = _ratio(row["errors"], row["success"])
            if r is not None:
                series[row["date_str"]] = r

        if len(series) < 4:
            continue

        pct, direction = _wow_trend(series, dates_no_sun)
        if direction != "growing":
            continue

        # Current ratio (avg last 7 non-sun days)
        avail = [d for d in dates_no_sun if d in series]
        curr_avg = round(sum(series[d] for d in avail[-7:]) / min(7, len(avail)), 1) if avail else None
        prev_avg = round(sum(series[d] for d in avail[-14:-7]) / min(7, len(avail[-14:-7])), 1) if len(avail) >= 8 else None

        # Total errors (for sorting)
        total_err = int(grp["errors"].sum())

        products_data.append({
            "name": str(prod),
            "segment": str(seg),
            "series": series,
            "pct": pct,
            "curr_avg": curr_avg,
            "prev_avg": prev_avg,
            "total_err": total_err,
        })

    # Sort by pct descending
    products_data.sort(key=lambda x: (x["pct"] or 0), reverse=True)

    if not products_data:
        _Path(output_path).write_text(_empty_html(), encoding="utf-8")
        return 0

    html = _render(products_data, dates_no_sun, df_full)
    _Path(output_path).write_text(html, encoding="utf-8")
    return len(products_data)


# ─── render ───────────────────────────────────────────────────────────────────

def _empty_html() -> str:
    return """<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8">
<title>Кандидаты</title></head><body style="font-family:sans-serif;padding:40px;">
<h2>Кандидаты</h2><p>Нет продуктов с растущим трендом ошибок.</p></body></html>"""


def _render(products: list, dates: list[str], df_full: pd.DataFrame) -> str:
    import uuid as _uuid
    uid = str(_uuid.uuid4())[:8]
    generated_at = _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    n = len(products)
    avg_ratio = round(sum(p["curr_avg"] or 0 for p in products) / n, 1) if n else 0

    # Build Plotly traces per product (ratio line)
    traces_js_parts = []
    vis_map = {}
    for i, p in enumerate(products):
        x = sorted(p["series"].keys())
        y = [p["series"][d] for d in x]
        visible = True if i == 0 else False
        vis_map[p["name"]] = [True if j == i else False for j in range(n)]
        trace = {
            "x": x, "y": y,
            "name": "Доля ошибок %",
            "type": "scatter", "mode": "lines+markers",
            "line": {"color": "#e74c3c", "width": 2},
            "marker": {"size": 5},
            "visible": visible,
            "hovertemplate": "%{x}: %{y:.1f}%<extra></extra>",
        }
        traces_js_parts.append(json.dumps(trace))

    traces_js = "[" + ",\n".join(traces_js_parts) + "]"
    vis_map_js = json.dumps(vis_map)

    # Dropdown options
    opts_html = ""
    for p in products:
        sign = "+" if (p["pct"] or 0) > 0 else ""
        pct_str = f"{sign}{p['pct']}%" if p["pct"] is not None else "—"
        opts_html += f'<option value="{p["name"]}">{p["name"]}  [{pct_str} WoW]</option>\n'

    # Table rows
    rows_html = ""
    for p in products:
        sign = "+" if (p["pct"] or 0) > 0 else ""
        pct_str = f"{sign}{p['pct']}%" if p["pct"] is not None else "—"
        curr_str = f"{p['curr_avg']}%" if p["curr_avg"] is not None else "—"
        prev_str = f"{p['prev_avg']}%" if p["prev_avg"] is not None else "—"
        rows_html += f"""<tr>
  <td><strong>{p['name']}</strong></td>
  <td style="color:#666;font-size:0.82rem;">{p['segment']}</td>
  <td style="text-align:right;">{p['total_err']:,}</td>
  <td style="text-align:right;color:#888;">{prev_str}</td>
  <td style="text-align:right;font-weight:600;">{curr_str}</td>
  <td style="text-align:right;font-weight:700;color:#e74c3c;">{pct_str}</td>
</tr>"""

    html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Кандидаты в проблемы</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f5f6fa; color: #2c3e50; }}
    .header {{ background: linear-gradient(135deg, #c0392b 0%, #e74c3c 100%); color: #fff; padding: 28px 40px; }}
    .header h1 {{ font-size: 1.6rem; font-weight: 700; }}
    .header .sub {{ opacity: .8; font-size: 0.85rem; margin-top: 4px; }}
    .container {{ max-width: 1400px; margin: 0 auto; padding: 32px 24px; }}
    .kpi-row {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 28px; }}
    .kpi-card {{ background: #fff; border-radius: 10px; padding: 18px 24px; flex: 1; min-width: 160px;
                 box-shadow: 0 2px 8px rgba(0,0,0,.06); }}
    .kpi-card .value {{ font-size: 2rem; font-weight: 700; }}
    .kpi-card .label {{ font-size: 0.78rem; color: #888; text-transform: uppercase; letter-spacing: .05em; margin-top: 4px; }}
    .section {{ background: #fff; border-radius: 12px; padding: 24px; box-shadow: 0 2px 8px rgba(0,0,0,.06); margin-bottom: 24px; }}
    .section h2 {{ font-size: 1.1rem; font-weight: 700; margin-bottom: 16px; color: #2c3e50; }}
    .drill-controls {{ display: flex; align-items: center; gap: 12px; margin-bottom: 16px; flex-wrap: wrap; }}
    .drill-controls label {{ font-size: 0.85rem; color: #666; font-weight: 600; }}
    .drill-controls select {{ padding: 6px 12px; border: 1px solid #ddd; border-radius: 6px;
                              font-size: 0.85rem; background: #fff; min-width: 280px; max-width: 480px; }}
    table.stat-table {{ width: 100%; border-collapse: collapse; font-size: 0.88rem; }}
    table.stat-table thead tr {{ background: #f8f9fa; }}
    table.stat-table th {{ padding: 10px 12px; text-align: left; font-size: 0.78rem; text-transform: uppercase;
                          color: #888; letter-spacing: .04em; border-bottom: 2px solid #e8ecf0; white-space: nowrap; }}
    table.stat-table td {{ padding: 10px 12px; border-bottom: 1px solid #f0f3f7; vertical-align: middle; }}
    table.stat-table tbody tr:hover {{ background: #f8faff; }}
    .badge-grow {{ background: #fdecea; color: #c0392b; padding: 2px 8px; border-radius: 12px; font-size: 0.78rem; font-weight: 600; }}
    .empty-state {{ text-align: center; padding: 40px; color: #888; font-size: 0.95rem; }}
  </style>
</head>
<body>
  <div class="header">
    <h1>🎯 Кандидаты в проблемы</h1>
    <div class="sub">Продукты с растущим трендом доли ошибок (55556 / 55558) — сформировано {generated_at}</div>
  </div>
  <div class="container">

    <!-- KPI -->
    <div class="kpi-row">
      <div class="kpi-card">
        <div class="value" style="color:#e74c3c;">{n}</div>
        <div class="label">Кандидатов</div>
      </div>
      <div class="kpi-card">
        <div class="value" style="color:#c0392b;">{avg_ratio}%</div>
        <div class="label">Ср. доля ошибок</div>
      </div>
      <div class="kpi-card">
        <div class="value" style="color:#888;">{dates[0] if dates else '—'}</div>
        <div class="label">Начало периода</div>
      </div>
      <div class="kpi-card">
        <div class="value" style="color:#888;">{dates[-1] if dates else '—'}</div>
        <div class="label">Конец периода</div>
      </div>
    </div>

    <!-- Chart drill-down -->
    <div class="section">
      <h2>📈 Динамика доли ошибок по продукту день за днём</h2>
      <div class="drill-controls">
        <label>Продукт:</label>
        <select id="cand_select_{uid}" onchange="candUpdate_{uid}(this.value)">
          {opts_html}
        </select>
      </div>
      <div id="cand_chart_{uid}" style="height:320px;"></div>
    </div>

    <!-- Table -->
    <div class="section">
      <h2>📋 Продукты с растущим трендом ({n})</h2>
      <div style="overflow-x:auto;">
      <table class="stat-table" id="cand_tbl_{uid}">
        <thead>
          <tr>
            <th style="cursor:pointer;" data-col="0">Продукт ⇅</th>
            <th style="cursor:pointer;" data-col="1">Сегмент ⇅</th>
            <th style="text-align:right;cursor:pointer;" data-col="2">Σ ошибок ⇅</th>
            <th style="text-align:right;cursor:pointer;" data-col="3">Пред. неделя ⇅</th>
            <th style="text-align:right;cursor:pointer;" data-col="4">Тек. неделя ⇅</th>
            <th style="text-align:right;cursor:pointer;" data-col="5">Тренд WoW ⇅</th>
          </tr>
        </thead>
        <tbody>
          {rows_html}
        </tbody>
      </table>
      </div>
    </div>

  </div>

  <script>
  (function() {{
    var TRACES = {traces_js};
    var VIS_MAP = {vis_map_js};

    var layout = {{
      margin: {{t:20, b:50, l:50, r:20}},
      plot_bgcolor: "#fff", paper_bgcolor: "#fff",
      xaxis: {{showgrid: true, gridcolor: "#f0f3f7"}},
      yaxis: {{showgrid: true, gridcolor: "#f0f3f7", title: "Доля ошибок %", rangemode: "tozero"}},
      hovermode: "x unified",
      legend: {{orientation: "h", y: -0.15}},
    }};

    Plotly.newPlot("cand_chart_{uid}", TRACES, layout, {{responsive: true, displayModeBar: false}});

    window.candUpdate_{uid} = function(prod) {{
      var vis = VIS_MAP[prod] || [];
      var update = {{visible: vis}};
      Plotly.restyle("cand_chart_{uid}", update);
    }};

    // Table sort
    (function() {{
      var tbl = document.getElementById("cand_tbl_{uid}");
      if (!tbl) return;
      var state = {{col: null, dir: null}};
      tbl.querySelectorAll("thead th[data-col]").forEach(function(th) {{
        th.addEventListener("click", function() {{
          var col = parseInt(th.getAttribute("data-col"));
          var dir = (state.col === col && state.dir === "asc") ? "desc" : "asc";
          var tbody = tbl.querySelector("tbody");
          var rows = Array.from(tbody.querySelectorAll("tr"));
          rows.sort(function(a, b) {{
            var av = (a.cells[col] || {{}}).textContent.trim().replace(/[+%,]/g,"");
            var bv = (b.cells[col] || {{}}).textContent.trim().replace(/[+%,]/g,"");
            var an = parseFloat(av), bn = parseFloat(bv);
            var cmp = (!isNaN(an) && !isNaN(bn)) ? an - bn : av.localeCompare(bv, "ru");
            return dir === "desc" ? -cmp : cmp;
          }});
          rows.forEach(function(r) {{ tbody.appendChild(r); }});
          state.col = col; state.dir = dir;
        }});
      }});
      // default sort by trend desc
      tbl.querySelector("thead th[data-col='5']").click();
      tbl.querySelector("thead th[data-col='5']").click();
    }})();
  }})();
  </script>
</body>
</html>"""
    return html
