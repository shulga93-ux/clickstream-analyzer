import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime
from pathlib import Path
from jinja2 import Template


def generate_report(df: pd.DataFrame, summary: dict, results: dict, output_path: str,
                    metric_id: str = "") -> str:
    """Generate a full HTML report for СБОЛ.про metrics format."""
    charts = _build_charts(df, results, metric_id=metric_id)
    html = _render_html(summary, results, charts, metric_id=metric_id)
    Path(output_path).write_text(html, encoding="utf-8")
    return output_path


# ─── Chart builders ──────────────────────────────────────────────────────────

def _build_charts(df: pd.DataFrame, results: dict, metric_id: str = "") -> dict:
    charts = {}
    COLORS = px.colors.qualitative.Set2
    COLORS_SEQ = px.colors.sequential.Blues

    # 1. Stacked bar: статусные экраны (55558) с типом "Ошибка" по дням, в разрезе блоков
    trends = results.get("trends", {})
    ts_by_metric = trends.get("timeseries", {})
    BLOCK_ORDER = ["боевой", "пилотный", "резервный", "неизвестный"]
    BLOCK_COLORS_TIMELINE = {
        "боевой":      "#e74c3c",
        "пилотный":    "#27ae60",
        "резервный":   "#3498db",
        "неизвестный": "#bbb",
    }

    # Filter: metric_id=55558, lvl_4="Ошибка"
    df_ss_err = df[
        (df["metric_id"].astype(str) == "55558") &
        (df["lvl_4"].astype(str) == "Ошибка")
    ].copy() if "lvl_4" in df.columns else pd.DataFrame()

    if not df_ss_err.empty:
        df_ss_err["date"] = df_ss_err["report_dt"].dt.date.astype(str)
        # Exclude Sundays
        df_ss_err = df_ss_err[df_ss_err["report_dt"].dt.weekday != 6]
        all_dates = sorted(df_ss_err["date"].unique())
        by_block_err = {}
        for bt, grp in df_ss_err.groupby("block_type"):
            day_vals = grp.groupby("date")["val"].sum().to_dict()
            by_block_err[bt] = day_vals

        fig = go.Figure()
        for bt in BLOCK_ORDER:
            if bt not in by_block_err:
                continue
            y_vals = [by_block_err[bt].get(d, 0) for d in all_dates]
            fig.add_trace(go.Bar(
                x=all_dates, y=y_vals, name=bt,
                marker_color=BLOCK_COLORS_TIMELINE.get(bt, "#aaa"),
            ))
        fig.update_layout(
            barmode="stack",
            title="Динамика ошибок статусного экрана по дням (в разрезе блоков, без воскресений)",
            height=380,
            legend=dict(orientation="h", y=-0.2),
            margin=dict(l=40, r=20, t=50, b=80),
            plot_bgcolor="#fafafa", paper_bgcolor="#ffffff",
            xaxis_title="Дата", yaxis_title="Кол-во ошибок (val)",
        )
        charts["timeline"] = fig.to_html(full_html=False, include_plotlyjs=False)
    else:
        # fallback: all errors by block_type
        ch_data = results.get("channel_breakdown", {})
        by_block = ch_data.get("by_block_per_day", {})
        block_dates = ch_data.get("dates", [])
        if by_block and block_dates:
            fig = go.Figure()
            for bt in BLOCK_ORDER:
                if bt not in by_block:
                    continue
                y_vals = [by_block[bt].get(d, 0) for d in block_dates]
                fig.add_trace(go.Bar(
                    x=block_dates, y=y_vals, name=bt,
                    marker_color=BLOCK_COLORS_TIMELINE.get(bt, "#aaa"),
                ))
            fig.update_layout(
                barmode="stack", title="Динамика ошибок по дням (в разрезе блоков)",
                height=380, legend=dict(orientation="h", y=-0.2),
                margin=dict(l=40, r=20, t=50, b=80),
                plot_bgcolor="#fafafa", paper_bgcolor="#ffffff",
                xaxis_title="Дата", yaxis_title="Кол-во событий (val)",
            )
            charts["timeline"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 1b. Error/Success ratio chart (55558: Ошибка / Успех per day, excl Sundays)
    if "lvl_4" in df.columns:
        df_ss = df[df["metric_id"].astype(str) == "55558"].copy()
        df_ss["date"] = df_ss["report_dt"].dt.date.astype(str)
        df_ss = df_ss[df_ss["report_dt"].dt.weekday != 6]  # excl Sundays
        if not df_ss.empty:
            daily_type = df_ss.groupby(["date", "lvl_4"])["val"].sum().reset_index()
            all_dates_ss = sorted(daily_type["date"].unique())
            err_map = daily_type[daily_type["lvl_4"] == "Ошибка"].set_index("date")["val"].to_dict()
            suc_map = daily_type[daily_type["lvl_4"] == "Успех"].set_index("date")["val"].to_dict()
            ratios, dates_ratio, err_vals, suc_vals = [], [], [], []
            for d in all_dates_ss:
                e = err_map.get(d, 0)
                s = suc_map.get(d, 0)
                if s > 0:
                    ratios.append(round(e / s * 100, 2))
                    dates_ratio.append(d)
                    err_vals.append(e)
                    suc_vals.append(s)
            if ratios:
                fig_r = go.Figure()
                fig_r.add_trace(go.Bar(
                    x=dates_ratio, y=err_vals, name="Ошибка",
                    marker_color="#e74c3c", opacity=0.5, yaxis="y2",
                ))
                fig_r.add_trace(go.Bar(
                    x=dates_ratio, y=suc_vals, name="Успех",
                    marker_color="#27ae60", opacity=0.5, yaxis="y2",
                ))
                fig_r.add_trace(go.Scatter(
                    x=dates_ratio, y=ratios, name="Ошибка / Успех (%)",
                    mode="lines+markers",
                    line=dict(color="#c0392b", width=2.5),
                    marker=dict(size=6),
                    yaxis="y",
                ))
                fig_r.update_layout(
                    barmode="group",
                    title="Статусный экран: отношение Ошибка / Успех по дням (%, без воскресений)",
                    height=400,
                    legend=dict(orientation="h", y=-0.2),
                    margin=dict(l=50, r=60, t=55, b=80),
                    plot_bgcolor="#fafafa", paper_bgcolor="#ffffff",
                    xaxis_title="Дата",
                    yaxis=dict(title="Ошибка / Успех (%)", side="left", color="#c0392b"),
                    yaxis2=dict(title="Кол-во событий", overlaying="y", side="right", showgrid=False),
                )
                charts["error_ratio"] = fig_r.to_html(full_html=False, include_plotlyjs=False)

    # 1c. Non-error status types chart (Успех / Ожидание / Информирование) by day, excl Sundays
    if "lvl_4" in df.columns:
        df_ss_other = df[
            (df["metric_id"].astype(str) == "55558") &
            (df["lvl_4"].astype(str) != "Ошибка")
        ].copy() if "lvl_4" in df.columns else pd.DataFrame()
        if not df_ss_other.empty and "report_dt" in df_ss_other.columns:
            df_ss_other["date"] = df_ss_other["report_dt"].dt.date
            df_ss_other = df_ss_other[df_ss_other["date"].apply(lambda d: d.weekday() != 6)]
            OTHER_COLORS = {
                "Успех":          "#27ae60",
                "Ожидание":       "#f39c12",
                "Информирование": "#3498db",
            }
            other_keys = ["Успех", "Ожидание", "Информирование"]
            fig_other = go.Figure()
            for key in other_keys:
                grp = df_ss_other[df_ss_other["lvl_4"].astype(str) == key]
                if grp.empty:
                    continue
                daily = grp.groupby("date")["val"].sum().reset_index().sort_values("date")
                daily["date"] = daily["date"].astype(str)
                fig_other.add_trace(go.Bar(
                    x=daily["date"], y=daily["val"], name=key,
                    marker_color=OTHER_COLORS.get(key, "#aaa"),
                ))
            fig_other.update_layout(
                barmode="stack",
                title="Динамика статусного экрана по дням — Успех / Ожидание / Информирование (без воскресений)",
                height=360,
                legend=dict(orientation="h", y=-0.2),
                margin=dict(l=40, r=20, t=50, b=80),
                plot_bgcolor="#fafafa",
                paper_bgcolor="#ffffff",
                xaxis_title="Дата",
                yaxis_title="Кол-во событий (val)",
            )
            charts["other_status_chart"] = fig_other.to_html(full_html=False, include_plotlyjs=False)

    def _group_by_product(items):
        """Aggregate deviations by (segment, lvl_2): sum val_curr/val_prev, recalc delta/pct."""
        from collections import defaultdict
        agg = defaultdict(lambda: {"val_curr": 0, "val_prev": 0, "segment": "", "date_from": "", "date_to": ""})
        for r in items:
            key = (r.get("segment_name", r.get("lvl_1", "")), r.get("lvl_2", ""))
            agg[key]["val_curr"] += r.get("val_curr", 0)
            agg[key]["val_prev"] += r.get("val_prev", 0)
            agg[key]["segment"] = r.get("segment_name", r.get("lvl_1", ""))
            agg[key]["date_from"] = r.get("date_from", "")
            agg[key]["date_to"] = r.get("date_to", "")
        result = []
        for (seg, prod), v in agg.items():
            delta = v["val_curr"] - v["val_prev"]
            pct = round((delta / v["val_prev"]) * 100, 1) if v["val_prev"] > 0 else 0
            result.append({
                "segment": seg, "lvl_2": prod,
                "val_curr": v["val_curr"], "val_prev": v["val_prev"],
                "delta": delta, "pct": pct,
                "date_from": v["date_from"], "date_to": v["date_to"],
            })
        return sorted(result, key=lambda x: abs(x["delta"]), reverse=True)

    # 2. WoW deviations — grouped by product, stored as data for HTML table
    wow = results.get("wow", [])
    wow_grouped = _group_by_product(wow) if wow else []

    # 3. DoD deviations — raw day-vs-same-weekday rows (no product grouping)
    dod = results.get("dod", [])

    # Store in charts dict as JSON-serialisable data (rendered via template table)
    charts["wow_grouped"] = wow_grouped[:20]
    charts["dod_grouped"] = dod[:50]   # top 50 already sorted by |delta|
    charts["wow_meta"] = {
        "curr_week_start": wow[0].get("curr_week_start", wow[0]["date_from"]),
        "curr_week_end":   wow[0].get("curr_week_end",   wow[0]["date_to"]),
        "prev_week_start": wow[0].get("prev_week_start", wow[0]["date_from"]),
        "prev_week_end":   wow[0].get("prev_week_end",   wow[0]["date_to"]),
    } if wow else {}
    charts["dod_meta"] = {
        "date_from": min(r["date_from"] for r in dod) if dod else "",
        "date_to":   max(r["date_to"]   for r in dod) if dod else "",
    }

    # 4. Top-20 services horizontal bar
    top_services = results.get("top_services", [])
    if top_services:
        top20 = top_services[:20]
        labels = [r["log_name"] for r in top20]
        vals = [r["total_val"] for r in top20]
        fig = go.Figure(go.Bar(
            y=labels[::-1],
            x=vals[::-1],
            orientation="h",
            marker_color="#4f8ef7",
            text=[f"{v:,}" for v in vals[::-1]],
            textposition="outside",
        ))
        fig.update_layout(
            title="Топ-20 сервисов по числу ошибок",
            height=max(400, len(top20) * 24),
            margin=dict(l=340, r=100, t=50, b=20),
            plot_bgcolor="#fafafa",
            paper_bgcolor="#ffffff",
            xaxis_title="Суммарный val",
        )
        charts["services_bar"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 5. Channel breakdown — stacked bar per channel by day
    ch_bd = results.get("channel_breakdown", {})
    by_channel_day = ch_bd.get("by_channel_per_day", {})
    if by_channel_day:
        fig = go.Figure()
        all_dates = sorted({d for v in by_channel_day.values() for d in v.keys()})
        for i, (ch, day_vals) in enumerate(by_channel_day.items()):
            y_vals = [day_vals.get(d, 0) for d in all_dates]
            fig.add_trace(go.Bar(
                x=all_dates, y=y_vals, name=ch,
                marker_color=COLORS[i % len(COLORS)],
            ))
        fig.update_layout(
            barmode="stack",
            title="Динамика по каналам",
            height=340,
            legend=dict(orientation="h", y=-0.2),
            margin=dict(l=40, r=20, t=50, b=80),
            plot_bgcolor="#fafafa",
            paper_bgcolor="#ffffff",
        )
        charts["channel_bar"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 6. Segment pie
    by_segment = ch_bd.get("by_segment_total", {})
    if by_segment:
        fig = go.Figure(go.Pie(
            labels=list(by_segment.keys()),
            values=list(by_segment.values()),
            hole=0.4,
            textinfo="label+percent",
            marker=dict(colors=COLORS),
        ))
        fig.update_layout(
            title="Распределение по сегментам (lvl_1)",
            height=320,
            margin=dict(l=20, r=20, t=50, b=20),
        )
        charts["segment_pie"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 7. Status screen pie (55558 by lvl_4)
    ss = results.get("status_screen", {})
    ss_total = ss.get("total", {})
    if ss_total:
        fig = go.Figure(go.Pie(
            labels=list(ss_total.keys()),
            values=list(ss_total.values()),
            hole=0.35,
            textinfo="label+percent+value",
            marker=dict(colors=px.colors.qualitative.Pastel),
        ))
        fig.update_layout(
            title="Статусный экран (55558): разбивка по типу",
            height=340,
            margin=dict(l=20, r=20, t=50, b=20),
        )
        charts["status_pie"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 8. Trends per metric (line chart)
    if ts_by_metric:
        import numpy as _np
        fig = go.Figure()
        per_metric = trends.get("per_metric", {})
        for i, (metric, records) in enumerate(ts_by_metric.items()):
            ts_df = pd.DataFrame(records)
            color = COLORS[i % len(COLORS)]
            y_vals = ts_df["val"].values.astype(float)
            x_vals = _np.arange(len(y_vals))
            x_labels = ts_df["date"].astype(str).tolist()

            # Actual data line
            fig.add_trace(go.Scatter(
                x=x_labels, y=y_vals,
                name=metric,
                mode="lines+markers",
                line=dict(color=color, width=2),
            ))

            # Trend line (linear regression)
            if len(y_vals) >= 3:
                slope = _np.polyfit(x_vals, y_vals, 1)
                y_trend = _np.polyval(slope, x_vals)
                mt = per_metric.get(metric, {})
                pct = mt.get("pct_change_total", 0)
                sign = "+" if pct > 0 else ""
                fig.add_trace(go.Scatter(
                    x=x_labels, y=y_trend,
                    name=f"{metric} тренд ({sign}{pct}%)",
                    mode="lines",
                    line=dict(color=color, width=2, dash="dash"),
                    opacity=0.6,
                    showlegend=True,
                ))

        fig.update_layout(
            title="Тренды по типам ошибок (без воскресений)",
            height=380,
            legend=dict(orientation="h", y=-0.2),
            margin=dict(l=40, r=20, t=50, b=80),
            plot_bgcolor="#fafafa",
            paper_bgcolor="#ffffff",
        )
        charts["trend_lines"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 8b. Weekly trends by channel and by block (excl. Sundays)
    wt = results.get("weekly_trends", {})
    weeks = wt.get("weeks", [])
    CH_COLORS = {"Web (АРМ)": "#e67e22", "iPad (Планшеты)": "#9b59b6"}
    BL_COLORS = {"боевой": "#e74c3c", "пилотный": "#27ae60", "резервный": "#3498db", "неизвестный": "#bbb"}

    if weeks:
        import numpy as _np2

        def _add_trendline(fig, x_labels, y_vals, color, name):
            """Add dashed linear trend line to figure."""
            n = len(y_vals)
            if n < 2:
                return
            x_num = _np2.arange(n, dtype=float)
            coeffs = _np2.polyfit(x_num, y_vals, 1)
            y_trend = _np2.polyval(coeffs, x_num)
            pct = round((y_trend[-1] - y_trend[0]) / y_trend[0] * 100, 1) if y_trend[0] != 0 else 0
            sign = "+" if pct > 0 else ""
            fig.add_trace(go.Scatter(
                x=x_labels, y=y_trend,
                name=f"{name} тренд ({sign}{pct}%)",
                mode="lines",
                line=dict(color=color, width=2, dash="dash"),
                opacity=0.55,
            ))

        # Channel weekly trend
        fig_ch = go.Figure()
        for ch, series in wt.get("by_channel", {}).items():
            ts = pd.DataFrame(series)
            color = CH_COLORS.get(ch, "#aaa")
            y_vals = ts["val"].values.astype(float)
            fig_ch.add_trace(go.Scatter(
                x=ts["week"], y=y_vals, name=ch, mode="lines+markers",
                line=dict(color=color, width=2), marker=dict(size=6),
            ))
            _add_trendline(fig_ch, ts["week"].tolist(), y_vals, color, ch)
        fig_ch.update_layout(
            title="Тренд ошибок по каналам (среднедневные, без воскресений)",
            height=340, legend=dict(orientation="h", y=-0.22),
            margin=dict(l=40, r=20, t=50, b=90),
            plot_bgcolor="#fafafa", paper_bgcolor="#ffffff",
            xaxis_title="Неделя", yaxis_title="Среднедневной val",
        )
        charts["weekly_channel"] = fig_ch.to_html(full_html=False, include_plotlyjs=False)

        # Block weekly trend
        fig_bl = go.Figure()
        BL_ORDER = ["боевой", "пилотный", "резервный", "неизвестный"]
        for bt in BL_ORDER:
            series = wt.get("by_block", {}).get(bt)
            if not series:
                continue
            ts = pd.DataFrame(series)
            color = BL_COLORS.get(bt, "#aaa")
            y_vals = ts["val"].values.astype(float)
            fig_bl.add_trace(go.Scatter(
                x=ts["week"], y=y_vals, name=bt, mode="lines+markers",
                line=dict(color=color, width=2), marker=dict(size=6),
            ))
            _add_trendline(fig_bl, ts["week"].tolist(), y_vals, color, bt)
        fig_bl.update_layout(
            title="Тренд ошибок по блокам (среднедневные, без воскресений)",
            height=340, legend=dict(orientation="h", y=-0.22),
            margin=dict(l=40, r=20, t=50, b=90),
            plot_bgcolor="#fafafa", paper_bgcolor="#ffffff",
            xaxis_title="Неделя", yaxis_title="Среднедневной val",
        )
        charts["weekly_block"] = fig_bl.to_html(full_html=False, include_plotlyjs=False)

    # 9. Product drill-down: dropdown selector → bar chart by block_type or lvl_4 per day
    pd_data = results.get("product_dynamics", {})
    block_matrix = pd_data.get("block_matrix", {})
    lvl4_matrix = pd_data.get("lvl4_matrix", {})
    channel_matrix = pd_data.get("channel_matrix", {})
    dates_pd = pd_data.get("dates", [])
    products_meta = pd_data.get("products", [])

    # 9a. All-types status drill (Ошибка + Успех + Ожидание + Информирование per product per day)
    use_lvl4_check = bool(lvl4_matrix and any(lvl4_matrix.values()))
    if use_lvl4_check and dates_pd and products_meta:
        ALL_LVL4_COLORS = {
            "Ошибка":         "#e74c3c",
            "Ожидание":       "#f39c12",
            "Информирование": "#3498db",
            "Успех":          "#27ae60",
        }
        ALL_KEYS = ["Успех", "Информирование", "Ожидание", "Ошибка"]

        all_traces_s = []
        vis_map_s = {}
        idx_s = 0

        for pm in products_meta:
            p = pm["name"]
            prod_vis = []
            for key in ALL_KEYS:
                kdata = lvl4_matrix.get(p, {}).get(key, {})
                y_vals = [kdata.get(d, 0) for d in dates_pd]
                if sum(y_vals) == 0:
                    continue
                is_first = (idx_s == sum(len(v) for v in vis_map_s.values()))
                tr = go.Bar(
                    x=dates_pd, y=y_vals, name=key,
                    marker_color=ALL_LVL4_COLORS.get(key, "#aaa"),
                    legendgroup=key,
                    showlegend=(len(vis_map_s) == 0),
                    visible=(len(vis_map_s) == 0),
                )
                all_traces_s.append(tr)
                prod_vis.append(idx_s)
                idx_s += 1
            vis_map_s[p] = prod_vis

        total_traces_s = idx_s
        if all_traces_s:
            first_pm = products_meta[0]
            fig_s = go.Figure(data=all_traces_s)
            fig_s.update_layout(
                title=f"📦 {first_pm['name']}  |  все типы",
                barmode="stack",
                height=420,
                margin=dict(l=40, r=40, t=60, b=60),
                plot_bgcolor="#fafafa",
                paper_bgcolor="#ffffff",
                xaxis_title="Дата",
                yaxis=dict(title="Кол-во событий (val)", side="left"),
                legend=dict(orientation="h", y=-0.18),
            )
            fig_s_html = fig_s.to_html(full_html=False, include_plotlyjs=False)

            import json as _json2
            js_data_s = {
                "visMap": vis_map_s,
                "totalTraces": total_traces_s,
                "products": {pm["name"]: {"total_val": pm["total_val"]} for pm in products_meta},
                "productOrder": [pm["name"] for pm in products_meta],
            }
            js_s_str = _json2.dumps(js_data_s, ensure_ascii=False)

            sel_opts_s = "".join(
                f'<option value="{pm["name"]}">{pm["name"]}</option>\n'
                for pm in products_meta
            )
            sdrill_uid = "sdrill_" + str(abs(hash(str(products_meta[0]["name"]))))[:8]

            charts["status_drill"] = f"""
<div id="{sdrill_uid}_wrap">
  <div style="margin-bottom:10px;">
    <label style="font-size:0.85rem;color:#555;margin-right:8px;font-weight:600;">Продукт:</label>
    <select id="{sdrill_uid}_sel" onchange="sdrillUpdate_{sdrill_uid}(this.value)"
            style="padding:6px 12px;border-radius:6px;border:1.5px solid #d0d7de;
                   font-size:0.9rem;min-width:340px;cursor:pointer;">
      {sel_opts_s}
    </select>
  </div>
  <div id="{sdrill_uid}_chart">{fig_s_html}</div>
</div>
<script>
(function() {{
  const UID = "{sdrill_uid}";
  const D = {js_s_str};
  function fmtNum(n) {{ return n == null ? "—" : n.toLocaleString("ru-RU"); }}
  function sdrillUpdate(name) {{
    const chartDiv = document.querySelector("#{sdrill_uid}_chart .js-plotly-plot");
    if (!chartDiv) return;
    const visIndices = D.visMap[name] || [];
    const vis = Array(D.totalTraces).fill(false);
    visIndices.forEach(i => vis[i] = true);
    const pm = D.products[name];
    Plotly.update(chartDiv,
      {{"visible": vis, "showlegend": vis}},
      {{"title.text": "📦 " + name + "  |  все типы"}}
    );
  }}
  window["sdrillUpdate_" + UID] = sdrillUpdate;
  document.addEventListener("DOMContentLoaded", function() {{
    const sel = document.getElementById(UID + "_sel");
    if (sel) sdrillUpdate(sel.value);
  }});
}})();
</script>
"""

    # Use lvl_4 breakdown if available (metric 55558), otherwise block_type
    use_lvl4 = bool(lvl4_matrix and any(lvl4_matrix.values()))
    primary_matrix = lvl4_matrix if use_lvl4 else block_matrix

    if primary_matrix and dates_pd and products_meta:
        BLOCK_COLORS = {
            "боевой":       "#e74c3c",
            "пилотный":     "#27ae60",
            "резервный":    "#3498db",
            "неизвестный":  "#aaa",
        }
        LVL4_COLORS = {
            "Ошибка":         "#e74c3c",
            "Ожидание":       "#f39c12",
            "Информирование": "#3498db",
            "Успех":          "#27ae60",
        }
        CHANNEL_COLORS = {
            "iPad (Планшеты)": "#9b59b6",
            "Web (АРМ)":       "#e67e22",
        }
        is_ss = (metric_id == "55558")
        if use_lvl4:
            primary_keys = ["Ошибка"] if is_ss else ["Ошибка", "Ожидание", "Информирование", "Успех"]
            primary_colors = LVL4_COLORS
            group_label = "Тип экрана"
        else:
            primary_keys = ["боевой", "пилотный", "резервный", "неизвестный"]
            primary_colors = BLOCK_COLORS
            group_label = "Тип блока"
        channel_names = ["Web (АРМ)", "iPad (Планшеты)"]

        traces = []
        buttons = []
        # Each product: traces for block_types + channels (all visible only for that product)
        traces_per_product = []

        for pm in products_meta:
            p = pm["name"]
            product_traces = []

            # Primary breakdown traces (block_type or lvl_4)
            for key in primary_keys:
                # For lvl4 mode use pre-filtered matrix (ensures all products have data)
                if use_lvl4 and key == "Ошибка":
                    kdata = pd_data.get("matrix", {}).get(p, {})
                else:
                    kdata = primary_matrix.get(p, {}).get(key, {})
                y_vals = [kdata.get(d, 0) for d in dates_pd]
                if sum(y_vals) == 0:
                    continue
                product_traces.append({
                    "x": dates_pd, "y": y_vals,
                    "name": key, "type": "bar",
                    "marker_color": primary_colors.get(key, "#aaa"),
                    "legendgroup": key,
                    "showlegend": True,
                })

            if is_ss:
                # 55558: ratio line Ошибка/Успех, no channel lines
                err_data = pd_data.get("matrix", {}).get(p, {})
                suc_data = pd_data.get("success_matrix", {}).get(p, {})
                ratio_vals = []
                for d in dates_pd:
                    e = err_data.get(d, 0)
                    s = suc_data.get(d, 0)
                    ratio_vals.append(round(e / s * 100, 1) if s > 0 else None)
                if any(v is not None and v > 0 for v in ratio_vals):
                    product_traces.append({
                        "x": dates_pd, "y": ratio_vals,
                        "name": "Ошибка/Успех (%)", "type": "scatter",
                        "mode": "lines+markers",
                        "line": {"color": "#1a3a6b", "width": 2, "dash": "dot"},
                        "marker": {"size": 5},
                        "legendgroup": "ratio",
                        "showlegend": True,
                        "yaxis": "y2",
                    })
            else:
                # Тех Ошибка: channel lines on secondary axis
                for ch in channel_names:
                    chdata = channel_matrix.get(p, {}).get(ch, {})
                    y_vals = [chdata.get(d, 0) for d in dates_pd]
                    if sum(y_vals) == 0:
                        continue
                    product_traces.append({
                        "x": dates_pd, "y": y_vals,
                        "name": ch, "type": "scatter",
                        "mode": "lines+markers",
                        "line": {"color": CHANNEL_COLORS.get(ch, "#888"), "width": 2, "dash": "dot"},
                        "marker": {"size": 5},
                        "legendgroup": ch,
                        "showlegend": True,
                        "yaxis": "y2",
                    })

            traces_per_product.append(product_traces)

        # Flatten all traces; first product visible
        all_traces = []
        visibility_map = []  # list of lists: which global indices belong to each product
        idx = 0
        for i, pt in enumerate(traces_per_product):
            vis_indices = list(range(idx, idx + len(pt)))
            visibility_map.append(vis_indices)
            for t in pt:
                is_visible = (i == 0)
                scatter_kwargs = {}
                if t["type"] == "bar":
                    tr = go.Bar(
                        x=t["x"], y=t["y"], name=t["name"],
                        marker_color=t["marker_color"],
                        legendgroup=t["legendgroup"],
                        showlegend=is_visible,
                        visible=is_visible,
                    )
                else:
                    tr = go.Scatter(
                        x=t["x"], y=t["y"], name=t["name"],
                        mode=t["mode"],
                        line=t["line"],
                        marker=t["marker"],
                        legendgroup=t["legendgroup"],
                        showlegend=is_visible,
                        visible=is_visible,
                        yaxis=t.get("yaxis", "y"),
                    )
                all_traces.append(tr)
                idx += 1

        total_traces = idx

        # Build visibility map per product (for JS)
        vis_map_js = {}
        for i, pm in enumerate(products_meta):
            vis_map_js[pm["name"]] = visibility_map[i]

        fig = go.Figure(data=all_traces)
        first = products_meta[0]
        fig.update_layout(
            title=f"📦 {first['name']}  |  Σ {first['total_val']:,}",
            barmode="stack",
            height=440,
            margin=dict(l=40, r=40, t=60, b=60),
            plot_bgcolor="#fafafa",
            paper_bgcolor="#ffffff",
            xaxis_title="Дата",
            yaxis=dict(title=f"Кол-во событий ({group_label})", side="left"),
            yaxis2=dict(title="Кол-во событий (каналы)", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", y=-0.18),
        )
        fig_html = fig.to_html(full_html=False, include_plotlyjs=False)

        # Prepare JS data: per-product metadata for table + visibility
        import json as _json
        js_data = {
            "visMap": vis_map_js,
            "totalTraces": total_traces,
            "products": {pm["name"]: {
                "total_val": pm["total_val"],
                "segment": pm["segment"],
                "metric_name": pm["metric_name"],
                "dod_pct": pm.get("dod_pct"),
                "dod_delta": pm.get("dod_delta"),
                "wow_pct": pm.get("wow_pct"),
                "wow_delta": pm.get("wow_delta"),
                "last_val": pm.get("last_val", 0),
                "last_date": pd_data.get("last_date", ""),
            } for pm in products_meta},
            "productOrder": [pm["name"] for pm in products_meta],
            "groupLabel": group_label,
        }
        js_data_str = _json.dumps(js_data, ensure_ascii=False)

        # Build select options
        select_opts = ""
        for pm in products_meta:
            wow_pct = pm.get("wow_pct")
            wow_str = f"WoW {wow_pct:+.1f}% ср/день" if wow_pct is not None else "WoW —"
            select_opts += f'<option value="{pm["name"]}">{pm["name"]}  [{wow_str}]</option>\n'

        drill_uid = "drill_" + str(abs(hash(str(products_meta[0]))))[:8]

        charts["product_drill"] = f"""
<div id="{drill_uid}_wrap">
  <div style="margin-bottom:10px;">
    <label style="font-size:0.85rem;color:#555;margin-right:8px;font-weight:600;">Продукт:</label>
    <select id="{drill_uid}_sel" onchange="drillUpdate_{drill_uid}(this.value)"
            style="padding:6px 12px;border-radius:6px;border:1.5px solid #d0d7de;
                   font-size:0.9rem;min-width:340px;cursor:pointer;">
      {select_opts}
    </select>
  </div>
  <div id="{drill_uid}_chart">{fig_html}</div>
  <div id="{drill_uid}_table" style="margin-top:16px;overflow-x:auto;"></div>
</div>
<script>
(function() {{
  const UID = "{drill_uid}";
  const D = {js_data_str};

  function fmtNum(n) {{ return n == null ? "—" : n.toLocaleString("ru-RU"); }}
  function fmtPct(p) {{ return p == null ? "—" : (p > 0 ? "+" : "") + p.toFixed(1) + "%"; }}
  function pctColor(p) {{ return p == null ? "#999" : (p > 0 ? "#e74c3c" : "#3498db"); }}

  function buildTable(name) {{
    const pm = D.products[name];
    if (!pm) return "";
    const dodColor = pctColor(pm.dod_pct);
    const wowColor = pctColor(pm.wow_pct);
    return `
      <table style="width:100%;border-collapse:collapse;font-size:0.88rem;">
        <thead>
          <tr style="background:#f8f9fa;text-transform:uppercase;font-size:0.74rem;color:#888;">
            <th style="padding:8px 10px;text-align:left;">Показатель</th>
            <th style="padding:8px 10px;text-align:right;">Значение</th>
          </tr>
        </thead>
        <tbody>
          <tr><td style="padding:7px 10px;border-bottom:1px solid #f0f2f5;">Продукт</td>
              <td style="padding:7px 10px;border-bottom:1px solid #f0f2f5;text-align:right;font-weight:700;">${{name}}</td></tr>
          <tr><td style="padding:7px 10px;border-bottom:1px solid #f0f2f5;">Сегмент</td>
              <td style="padding:7px 10px;border-bottom:1px solid #f0f2f5;text-align:right;">${{pm.segment}}</td></tr>
          <tr><td style="padding:7px 10px;border-bottom:1px solid #f0f2f5;">Суммарный val (период)</td>
              <td style="padding:7px 10px;border-bottom:1px solid #f0f2f5;text-align:right;font-weight:700;">${{fmtNum(pm.total_val)}}</td></tr>
          <tr><td style="padding:7px 10px;">WoW ${{D.groupLabel}} — неделя к неделе</td>
              <td style="padding:7px 10px;text-align:right;font-weight:700;color:${{wowColor}};">
                ${{pm.wow_delta != null ? (pm.wow_delta > 0 ? "+" : "") + fmtNum(pm.wow_delta) + " (" + fmtPct(pm.wow_pct) + ")" : "—"}}
              </td></tr>
        </tbody>
      </table>`;
  }}

  function updatePlotly(name) {{
    const chartDiv = document.querySelector("#{drill_uid}_chart .js-plotly-plot");
    if (!chartDiv) return;
    const visIndices = D.visMap[name] || [];
    const vis = Array(D.totalTraces).fill(false);
    visIndices.forEach(i => vis[i] = true);
    const pm = D.products[name];
    Plotly.update(chartDiv,
      {{"visible": vis, "showlegend": vis}},
      {{"title.text": "📦 " + name + "  |  Σ " + fmtNum(pm.total_val)}}
    );
  }}

  window["drillUpdate_" + UID] = function(name) {{
    updatePlotly(name);
    document.getElementById(UID + "_table").innerHTML = buildTable(name);
  }};

  // Init on load
  document.addEventListener("DOMContentLoaded", function() {{
    const sel = document.getElementById(UID + "_sel");
    if (sel) {{
      window["drillUpdate_" + UID](sel.value);
    }}
  }});
}})();
</script>
"""

    return charts


# ─── HTML rendering ──────────────────────────────────────────────────────────

def _render_html(summary: dict, results: dict, charts: dict, metric_id: str = "") -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    anomalies = sorted(results.get("anomalies", []), key=lambda a: a.get("timestamp", ""))
    wow = results.get("wow", [])
    dod = results.get("dod", [])
    trends = results.get("trends", {})
    top_services = results.get("top_services", [])
    ss = results.get("status_screen", {})

    pd_data = results.get("product_dynamics", {})
    wow_grouped = charts.pop("wow_grouped", [])
    dod_grouped = charts.pop("dod_grouped", [])
    wow_meta = charts.pop("wow_meta", {})
    dod_meta = charts.pop("dod_meta", {})

    template = Template(HTML_TEMPLATE)
    return template.render(
        generated_at=generated_at,
        summary=summary,
        anomalies=anomalies,
        wow=wow,
        dod=dod,
        wow_grouped=wow_grouped,
        dod_grouped=dod_grouped,
        wow_meta=wow_meta,
        dod_meta=dod_meta,
        trends=trends,
        top_services=top_services,
        ss=ss,
        charts=charts,
        pd_data=pd_data,
        metric_id=metric_id,
        is_status_screen=(metric_id == "55558"),
    )


# ─── HTML Template ────────────────────────────────────────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>СБОЛ.про — Анализ ошибок</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
           background: #f0f2f5; color: #1a1a2e; line-height: 1.6; }
    .header { background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
              color: white; padding: 40px; }
    .header h1 { font-size: 2rem; font-weight: 700; margin-bottom: 8px; }
    .header .meta { opacity: 0.75; font-size: 0.9rem; }
    .container { max-width: 1400px; margin: 0 auto; padding: 24px; }
    .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(148px, 1fr));
                gap: 14px; margin-bottom: 24px; }
    .kpi-card { background: white; border-radius: 12px; padding: 18px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.08); text-align: center; }
    .kpi-card .value { font-size: 1.75rem; font-weight: 700; color: #4f8ef7; }
    .kpi-card .label { font-size: 0.76rem; color: #888; text-transform: uppercase;
                       letter-spacing: 0.5px; margin-top: 4px; }
    .section { background: white; border-radius: 12px; padding: 24px;
               margin-bottom: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
    .section h2 { font-size: 1.1rem; font-weight: 600; margin-bottom: 16px;
                  padding-bottom: 10px; border-bottom: 2px solid #f0f2f5; }
    .chart-card { background: white; border-radius: 12px; padding: 16px;
                  box-shadow: 0 2px 8px rgba(0,0,0,0.08); margin-bottom: 20px; }
    .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
    .three-col { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; margin-bottom: 20px; }
    .stat-table { width: 100%; border-collapse: collapse; font-size: 0.86rem; }
    .stat-table th { text-align: left; padding: 8px 10px; background: #f8f9fa;
                     font-size: 0.76rem; text-transform: uppercase; color: #888; }
    .stat-table td { padding: 7px 10px; border-bottom: 1px solid #f0f2f5; }
    .stat-table tr:last-child td { border-bottom: none; }
    .dev-tbl td, .dev-tbl th { padding: 6px 8px; font-size: 0.83rem; }
    .dev-tbl tr:hover td { background: #f8f9fa; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 10px;
             font-size: 0.72rem; font-weight: 600; }
    .badge-red    { background: #ffe0e0; color: #c0392b; }
    .badge-orange { background: #fff3cd; color: #d68910; }
    .badge-green  { background: #e0f7ea; color: #1e8449; }
    .badge-blue   { background: #e0f0ff; color: #2471a3; }
    .deviation-row { padding: 8px 10px; border-radius: 6px; margin-bottom: 6px;
                     border-left: 3px solid; font-size: 0.84rem; }
    .dev-spike { background: #fff5f5; border-color: #e74c3c; }
    .dev-drop  { background: #f0f8ff; border-color: #3498db; }
    .trend-row { display: flex; align-items: center; gap: 12px; padding: 8px 0;
                 border-bottom: 1px solid #f0f2f5; }
    .trend-row:last-child { border-bottom: none; }
    .empty-state { text-align: center; padding: 24px; color: #aaa; }
    .footer { text-align: center; padding: 28px; color: #aaa; font-size: 0.85rem; }
    @media (max-width: 900px) {
      .two-col, .three-col { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>

<div class="header">
  <h1>📊 СБОЛ.про — Анализ ошибок</h1>
  <div class="meta">
    Период: {{ summary.date_range.start }} → {{ summary.date_range.end }}
    &nbsp;·&nbsp; Сгенерирован: {{ generated_at }}
  </div>
</div>

<div class="container">

  <!-- KPI Cards -->
  <div class="kpi-grid">
    <div class="kpi-card">
      <div class="value">{{ "{:,}".format(summary.total_records) }}</div>
      <div class="label">Строк данных</div>
    </div>
    <div class="kpi-card">
      <div class="value">{{ "{:,}".format(summary.total_val) }}</div>
      <div class="label">Суммарный val</div>
    </div>
    <div class="kpi-card">
      <div class="value">{{ summary.unique_dates }}</div>
      <div class="label">Дней в периоде</div>
    </div>
    <div class="kpi-card">
      <div class="value">{{ summary.unique_products }}</div>
      <div class="label">Продуктов (lvl_2)</div>
    </div>
    <div class="kpi-card">
      <div class="value">{{ summary.unique_services }}</div>
      <div class="label">Сервисов (log_name)</div>
    </div>

    <div class="kpi-card">
      <div class="value" style="color: #f39c12;">{{ wow_grouped | length }}</div>
      <div class="label">WoW продуктов</div>
    </div>
    <div class="kpi-card">
      <div class="value" style="color: #e67e22;">{{ dod_grouped | length }}</div>
      <div class="label">DoD отклонений</div>
    </div>
  </div>

  <!-- Timeline chart -->
  {% if charts.timeline %}
  <div class="chart-card">{{ charts.timeline }}</div>
  {% endif %}

  {# ─── Macro: WoW table (sortable) ─── #}
  {% macro wow_table(rows) %}
  {% if rows %}
  {% set max_delta = namespace(v=1) %}
  {% for r in rows %}{% if r.delta | abs > max_delta.v %}{% set max_delta.v = r.delta | abs %}{% endif %}{% endfor %}
  <div style="overflow-x:auto;">
  <table class="stat-table dev-tbl" id="wow_dev_tbl">
    <thead>
      <tr>
        <th style="width:34%;cursor:pointer;user-select:none;white-space:nowrap;" data-col="0">Продукт <span class="si">⇅</span></th>
        <th style="cursor:pointer;user-select:none;white-space:nowrap;" data-col="1">Сегмент <span class="si">⇅</span></th>
        <th style="text-align:right;cursor:pointer;user-select:none;white-space:nowrap;" data-col="2">Пред. неделя (Σ ошибок) <span class="si">⇅</span></th>
        <th style="text-align:right;cursor:pointer;user-select:none;white-space:nowrap;" data-col="3">Тек. неделя (Σ ошибок) <span class="si">⇅</span></th>
        <th style="text-align:right;cursor:pointer;user-select:none;white-space:nowrap;" data-col="4">Δval <span class="si">⇅</span></th>
        <th style="text-align:right;cursor:pointer;user-select:none;white-space:nowrap;" data-col="5">% <span class="si">⇅</span></th>
        <th style="width:80px;"></th>
      </tr>
    </thead>
    <tbody>
    {% for r in rows %}
    {% set is_up = r.delta > 0 %}
    {% set bar_w = ((r.delta | abs) / max_delta.v * 76) | int %}
    <tr>
      <td data-val="{{ r.lvl_2 }}"><strong>{{ r.lvl_2 }}</strong></td>
      <td data-val="{{ r.segment }}" style="color:#666;font-size:0.82rem;">{{ r.segment }}</td>
      <td data-val="{{ r.val_prev }}" style="text-align:right;color:#888;">{{ "{:,}".format(r.val_prev) }}</td>
      <td data-val="{{ r.val_curr }}" style="text-align:right;">{{ "{:,}".format(r.val_curr) }}</td>
      <td data-val="{{ r.delta }}" style="text-align:right;font-weight:700;color:{{ '#e74c3c' if is_up else '#3498db' }};">
        {{ '+' if is_up else '' }}{{ "{:,}".format(r.delta) }}
      </td>
      <td data-val="{{ r.pct }}" style="text-align:right;font-weight:600;color:{{ '#e74c3c' if is_up else '#3498db' }};">
        {{ '+' if is_up else '' }}{{ r.pct }}%
      </td>
      <td>
        <div style="background:{{ '#e74c3c' if is_up else '#3498db' }};height:10px;border-radius:3px;width:{{ bar_w }}px;min-width:2px;"></div>
      </td>
    </tr>
    {% endfor %}
    </tbody>
  </table>
  </div>
  <script>
  (function() {
    var TID = "wow_dev_tbl";
    var sortState = { col: null, dir: null };
    function updateIcons(tbl, activeCol, dir) {
      tbl.querySelectorAll("thead th[data-col] .si").forEach(function(span) {
        var col = parseInt(span.parentNode.getAttribute("data-col"));
        span.textContent = col === activeCol ? (dir === "desc" ? " ▼" : " ▲") : " ⇅";
        span.style.color = col === activeCol ? "#4f8ef7" : "#bbb";
      });
    }
    function sortTable(tbl, col, dir) {
      var tbody = tbl.querySelector("tbody");
      var rows = Array.from(tbody.querySelectorAll("tr"));
      rows.sort(function(a, b) {
        var av = a.cells[col] ? (a.cells[col].getAttribute("data-val") || "") : "";
        var bv = b.cells[col] ? (b.cells[col].getAttribute("data-val") || "") : "";
        var isDate = /^\\d{4}-\\d{2}-\\d{2}$/.test(av) && /^\\d{4}-\\d{2}-\\d{2}$/.test(bv);
        var an = parseFloat(av), bn = parseFloat(bv);
        var cmp = isDate ? av.localeCompare(bv) : ((!isNaN(an) && !isNaN(bn)) ? an - bn : av.localeCompare(bv, "ru"));
        return dir === "desc" ? -cmp : cmp;
      });
      rows.forEach(function(r) { tbody.appendChild(r); });
      sortState.col = col; sortState.dir = dir;
      updateIcons(tbl, col, dir);
    }
    document.addEventListener("DOMContentLoaded", function() {
      var tbl = document.getElementById(TID);
      if (!tbl) return;
      tbl.querySelectorAll("thead th[data-col]").forEach(function(th) {
        th.addEventListener("click", function() {
          var col = parseInt(th.getAttribute("data-col"));
          var dir = (sortState.col === col && sortState.dir === "asc") ? "desc" : "asc";
          sortTable(tbl, col, dir);
        });
      });
      sortTable(tbl, 3, "desc");
    });
  })();
  </script>
  {% else %}
  <div class="empty-state">✅ WoW отклонений нет</div>
  {% endif %}
  {% endmacro %}

  {# ─── Macro: DoD table (sortable, day/date column) ─── #}
  {% macro dod_table(rows) %}
  {% if rows %}
  {% set max_delta = namespace(v=1) %}
  {% for r in rows %}{% if r.delta | abs > max_delta.v %}{% set max_delta.v = r.delta | abs %}{% endif %}{% endfor %}
  <div style="overflow-x:auto;">
  <table class="stat-table dev-tbl" id="dod_dev_tbl">
    <thead>
      <tr>
        <th style="cursor:pointer;user-select:none;white-space:nowrap;" data-col="0">День / Дата <span class="si">⇅</span></th>
        <th style="width:28%;cursor:pointer;user-select:none;white-space:nowrap;" data-col="1">Продукт <span class="si">⇅</span></th>
        <th style="cursor:pointer;user-select:none;white-space:nowrap;" data-col="2">Сегмент <span class="si">⇅</span></th>
        <th style="text-align:right;cursor:pointer;user-select:none;white-space:nowrap;" data-col="3">Пред. неделя <span class="si">⇅</span></th>
        <th style="text-align:right;cursor:pointer;user-select:none;white-space:nowrap;" data-col="4">Тек. день <span class="si">⇅</span></th>
        <th style="text-align:right;cursor:pointer;user-select:none;white-space:nowrap;" data-col="5">Δval <span class="si">⇅</span></th>
        <th style="text-align:right;cursor:pointer;user-select:none;white-space:nowrap;" data-col="6">% <span class="si">⇅</span></th>
        <th style="width:70px;"></th>
      </tr>
    </thead>
    <tbody>
    {% for r in rows %}
    {% set is_up = r.delta > 0 %}
    {% set bar_w = ((r.delta | abs) / max_delta.v * 68) | int %}
    <tr>
      <td data-val="{{ r.date_to }}" style="white-space:nowrap;font-size:0.82rem;">
        <strong>{{ r.weekday }}</strong><br><span style="color:#888;">{{ r.date_to }}</span>
      </td>
      <td data-val="{{ r.lvl_2 }}"><strong>{{ r.lvl_2 }}</strong></td>
      <td data-val="{{ r.segment }}" style="color:#666;font-size:0.82rem;">{{ r.segment }}</td>
      <td data-val="{{ r.val_prev }}" style="text-align:right;color:#888;">{{ "{:,}".format(r.val_prev) }}</td>
      <td data-val="{{ r.val_curr }}" style="text-align:right;">{{ "{:,}".format(r.val_curr) }}</td>
      <td data-val="{{ r.delta }}" style="text-align:right;font-weight:700;color:{{ '#e74c3c' if is_up else '#3498db' }};">
        {{ '+' if is_up else '' }}{{ "{:,}".format(r.delta) }}
      </td>
      <td data-val="{{ r.pct }}" style="text-align:right;font-weight:600;color:{{ '#e74c3c' if is_up else '#3498db' }};">
        {{ '+' if is_up else '' }}{{ r.pct }}%
      </td>
      <td>
        <div style="background:{{ '#e74c3c' if is_up else '#3498db' }};height:10px;border-radius:3px;width:{{ bar_w }}px;min-width:2px;"></div>
      </td>
    </tr>
    {% endfor %}
    </tbody>
  </table>
  </div>
  <script>
  (function() {
    var TID = "dod_dev_tbl";
    var sortState = { col: null, dir: null };
    function updateIcons(tbl, activeCol, dir) {
      tbl.querySelectorAll("thead th[data-col] .si").forEach(function(span) {
        var col = parseInt(span.parentNode.getAttribute("data-col"));
        span.textContent = col === activeCol ? (dir === "desc" ? " ▼" : " ▲") : " ⇅";
        span.style.color = col === activeCol ? "#4f8ef7" : "#bbb";
      });
    }
    function sortTable(tbl, col, dir) {
      var tbody = tbl.querySelector("tbody");
      var rows = Array.from(tbody.querySelectorAll("tr"));
      rows.sort(function(a, b) {
        var av = a.cells[col] ? (a.cells[col].getAttribute("data-val") || "") : "";
        var bv = b.cells[col] ? (b.cells[col].getAttribute("data-val") || "") : "";
        var isDate = /^\\d{4}-\\d{2}-\\d{2}$/.test(av) && /^\\d{4}-\\d{2}-\\d{2}$/.test(bv);
        var an = parseFloat(av), bn = parseFloat(bv);
        var cmp = isDate ? av.localeCompare(bv) : ((!isNaN(an) && !isNaN(bn)) ? an - bn : av.localeCompare(bv, "ru"));
        return dir === "desc" ? -cmp : cmp;
      });
      rows.forEach(function(r) { tbody.appendChild(r); });
      sortState.col = col; sortState.dir = dir;
      updateIcons(tbl, col, dir);
    }
    document.addEventListener("DOMContentLoaded", function() {
      var tbl = document.getElementById(TID);
      if (!tbl) return;
      tbl.querySelectorAll("thead th[data-col]").forEach(function(th) {
        th.addEventListener("click", function() {
          var col = parseInt(th.getAttribute("data-col"));
          var dir = (sortState.col === col && sortState.dir === "asc") ? "desc" : "asc";
          sortTable(tbl, col, dir);
        });
      });
      sortTable(tbl, 5, "desc");
    });
  })();
  </script>
  {% else %}
  <div class="empty-state">✅ DoD отклонений нет</div>
  {% endif %}
  {% endmacro %}

  <!-- WoW section (full width) -->
  <div class="section">
    <h2>📅 Топ-20 продуктов с WoW отклонениями — неделя к неделе
      {% if wow_meta %}<span style="font-size:0.75rem;font-weight:400;color:#888;margin-left:8px;">
        Тек.: {{ wow_meta.curr_week_start }}–{{ wow_meta.curr_week_end }}
        &nbsp;vs&nbsp;
        Пред.: {{ wow_meta.prev_week_start }}–{{ wow_meta.prev_week_end }}
      </span>{% endif %}
    </h2>
    {% if wow_grouped %}
      {{ wow_table(wow_grouped) }}
    {% else %}
      <div class="empty-state">✅ WoW отклонений нет (нет данных за предыдущие 7 дней или ниже порога)</div>
    {% endif %}
  </div>

  <!-- Error/Success ratio chart -->
  {% if charts.error_ratio %}
  <div class="section">
    <h2>📉 Статусный экран — динамика отношения Ошибка / Успех</h2>
    {{ charts.error_ratio }}
  </div>
  {% endif %}

  {% if charts.other_status_chart %}
  <div class="section">
    <h2>✅ Статусный экран — динамика Успех / Ожидание / Информирование</h2>
    {{ charts.other_status_chart }}
  </div>
  {% endif %}

  <!-- DoD section (full width) -->
  <div class="section">
    <h2>📆 Топ-50 DoD отклонений — день недели к тому же дню предыдущей недели
      {% if dod_meta %}<span style="font-size:0.75rem;font-weight:400;color:#888;margin-left:8px;">{{ dod_meta.date_from }} – {{ dod_meta.date_to }}</span>{% endif %}
    </h2>
    {% if dod_grouped %}
      {{ dod_table(dod_grouped) }}
    {% else %}
      <div class="empty-state">✅ DoD отклонений нет (ниже порога >50% или нет пар с предыдущей неделей)</div>
    {% endif %}
  </div>

  <!-- Product drill-down: dropdown → bar by block_type + channel lines -->
  {% if is_status_screen and charts.status_drill %}
  <div class="section">
    <h2>📊 Динамика статусных экранов по продукту день за днём</h2>
    <p style="color:#888;font-size:0.85rem;margin-bottom:12px;">
      Все типы: Успех / Ожидание / Информирование / Ошибка — стековые столбцы по дням.
    </p>
    {{ charts.status_drill }}
  </div>
  {% endif %}

  {% if charts.product_drill %}
  <div class="section">
    <h2>🗓️ {% if is_status_screen %}Динамика ОШИБОК{% else %}Динамика{% endif %} по продукту день за днём</h2>
    <p style="color:#888;font-size:0.85rem;margin-bottom:12px;">
      {% if is_status_screen %}Выберите продукт — динамика ошибок (столбцы) и Ошибка/Успех % (пунктирная линия).{% else %}Выберите продукт — динамика ошибок по блокам и каналам.{% endif %}
    </p>
    {{ charts.product_drill }}
    {% if pd_data.products %}
    <h3 style="font-size:0.95rem;font-weight:600;margin:20px 0 10px;color:#444;">📊 Все каналы</h3>
    <div style="overflow-x:auto;">
    <table class="stat-table" id="pd_all_tbl" style="width:100%;">
      <thead>
        <tr>
          <th style="cursor:pointer;user-select:none;white-space:nowrap;" data-col="0">Продукт <span class="si">⇅</span></th>
          <th style="cursor:pointer;user-select:none;white-space:nowrap;" data-col="1">Сегмент <span class="si">⇅</span></th>
          <th style="text-align:right;cursor:pointer;user-select:none;white-space:nowrap;" data-col="2">Σ ошибок <span class="si">⇅</span></th>
          <th style="text-align:center;cursor:pointer;user-select:none;white-space:nowrap;" data-col="3">Тренд ошибок <span class="si">⇅</span></th>
          <th style="text-align:right;cursor:pointer;user-select:none;white-space:nowrap;" data-col="4">Доля ошибок % <span class="si">⇅</span></th>
          <th style="text-align:center;cursor:pointer;user-select:none;white-space:nowrap;" data-col="5">Тренд доли <span class="si">⇅</span></th>
        </tr>
      </thead>
      <tbody>
      {% for p in pd_data.products %}
      {% set td = p.trend_direction if p.trend_direction is defined else 'stable' %}
      {% set tp = p.trend_pct if p.trend_pct is defined else 0 %}
      {% set trend_icon  = '📈' if td == 'growing' else ('📉' if td == 'declining' else '→') %}
      {% set trend_color = '#e74c3c' if td == 'growing' else ('#27ae60' if td == 'declining' else '#888') %}
      {% set trend_label = 'Растёт' if td == 'growing' else ('Падает' if td == 'declining' else 'Стабильно') %}
      {% set er = p.err_suc_ratio if p.err_suc_ratio is defined else none %}
      {% set etd = p.err_suc_trend_dir if p.err_suc_trend_dir is defined else 'stable' %}
      {% set etp = p.err_suc_trend_pct if p.err_suc_trend_pct is defined else none %}
      {% set et_icon  = '📈' if etd == 'growing' else ('📉' if etd == 'declining' else '→') %}
      {% set et_color = '#e74c3c' if etd == 'growing' else ('#27ae60' if etd == 'declining' else '#888') %}
      {% set et_label = 'Растёт' if etd == 'growing' else ('Падает' if etd == 'declining' else 'Стаб.') %}
      <tr>
        <td data-val="{{ p.name }}" style="font-size:0.83rem;"><strong>{{ p.name }}</strong></td>
        <td data-val="{{ p.segment }}" style="color:#666;font-size:0.8rem;">{{ p.segment }}</td>
        <td data-val="{{ p.total_val }}" style="text-align:right;">{{ "{:,}".format(p.total_val) }}</td>
        <td data-val="{{ tp }}" style="text-align:center;">
          <span style="color:{{ trend_color }};font-weight:600;white-space:nowrap;font-size:0.85rem;">
            {{ trend_icon }} {{ trend_label }}
            {% if td != 'stable' and tp != 0 %}<span style="font-size:0.78rem;margin-left:3px;">({{ '+' if tp > 0 else '' }}{{ tp }}%)</span>{% endif %}
          </span>
        </td>
        <td data-val="{{ er if er is not none else -1 }}" style="text-align:right;font-weight:600;">
          {% if er is not none %}{{ er }}%{% else %}—{% endif %}
        </td>
        <td data-val="{{ etp if etp is not none else 0 }}" style="text-align:center;">
          <span style="color:{{ et_color }};font-weight:600;white-space:nowrap;font-size:0.85rem;">
            {{ et_icon }} {{ et_label }}
            {% if etd != 'stable' and etp is not none %}<span style="font-size:0.78rem;margin-left:3px;">({{ '+' if etp > 0 else '' }}{{ etp }}%)</span>{% endif %}
          </span>
        </td>
      </tr>
      {% endfor %}
      </tbody>
    </table>
    </div>
    <script>
    (function() {
      var TID = "pd_all_tbl";
      var ss = { col: null, dir: null };
      function upd(tbl, col, dir) {
        tbl.querySelectorAll("thead th[data-col] .si").forEach(function(s) {
          var c = parseInt(s.parentNode.getAttribute("data-col"));
          s.textContent = c === col ? (dir === "desc" ? " ▼" : " ▲") : " ⇅";
          s.style.color  = c === col ? "#4f8ef7" : "#bbb";
        });
      }
      function srt(tbl, col, dir) {
        var tbody = tbl.querySelector("tbody");
        var rows = Array.from(tbody.querySelectorAll("tr"));
        rows.sort(function(a, b) {
          var av = a.cells[col] ? (a.cells[col].getAttribute("data-val") || "") : "";
          var bv = b.cells[col] ? (b.cells[col].getAttribute("data-val") || "") : "";
          var an = parseFloat(av), bn = parseFloat(bv);
          var cmp = (!isNaN(an) && !isNaN(bn)) ? an - bn : av.localeCompare(bv, "ru");
          return dir === "desc" ? -cmp : cmp;
        });
        rows.forEach(function(r) { tbody.appendChild(r); });
        ss.col = col; ss.dir = dir; upd(tbl, col, dir);
      }
      document.addEventListener("DOMContentLoaded", function() {
        var tbl = document.getElementById(TID);
        if (!tbl) return;
        tbl.querySelectorAll("thead th[data-col]").forEach(function(th) {
          th.addEventListener("click", function() {
            var col = parseInt(th.getAttribute("data-col"));
            srt(tbl, col, (ss.col === col && ss.dir === "asc") ? "desc" : "asc");
          });
        });
        srt(tbl, 2, "desc");
      });
    })();
    </script>
    {% endif %}

    {% set ch_data = pd_data.by_channel_products if pd_data.by_channel_products is defined else {} %}
    {% if ch_data %}
    {# Macro: channel product table #}
    {% macro ch_table(rows, tbl_id) %}
    {% if rows %}
    <table class="stat-table" id="{{ tbl_id }}" style="width:100%;">
      <thead>
        <tr>
          <th style="cursor:pointer;user-select:none;white-space:nowrap;" data-col="0">Продукт <span class="si">⇅</span></th>
          <th style="cursor:pointer;user-select:none;white-space:nowrap;" data-col="1">Сегмент <span class="si">⇅</span></th>
          <th style="text-align:right;cursor:pointer;user-select:none;white-space:nowrap;" data-col="2">Σ ошибок <span class="si">⇅</span></th>
          <th style="text-align:center;cursor:pointer;user-select:none;white-space:nowrap;" data-col="3">Тренд ошибок <span class="si">⇅</span></th>
          <th style="text-align:right;cursor:pointer;user-select:none;white-space:nowrap;" data-col="4">Доля ошибок % <span class="si">⇅</span></th>
          <th style="text-align:center;cursor:pointer;user-select:none;white-space:nowrap;" data-col="5">Тренд доли <span class="si">⇅</span></th>
        </tr>
      </thead>
      <tbody>
      {% for p in rows %}
      {% set td = p.trend_direction %}
      {% set tp = p.trend_pct %}
      {% set trend_icon  = '📈' if td == 'growing' else ('📉' if td == 'declining' else '→') %}
      {% set trend_color = '#e74c3c' if td == 'growing' else ('#27ae60' if td == 'declining' else '#888') %}
      {% set trend_label = 'Растёт' if td == 'growing' else ('Падает' if td == 'declining' else 'Стабильно') %}
      {% set _esl = pd_data.err_suc_lookup if pd_data.err_suc_lookup is defined else {} %}
      {% set _ese = _esl.get(p.name, {}) if _esl else {} %}
      {% set er = _ese.ratio if _ese.ratio is defined else none %}
      {% set etd = _ese.trend_dir if _ese.trend_dir is defined else 'stable' %}
      {% set etp = _ese.trend_pct if _ese.trend_pct is defined else none %}
      {% set et_icon  = '📈' if etd == 'growing' else ('📉' if etd == 'declining' else '→') %}
      {% set et_color = '#e74c3c' if etd == 'growing' else ('#27ae60' if etd == 'declining' else '#888') %}
      {% set et_label = 'Растёт' if etd == 'growing' else ('Падает' if etd == 'declining' else 'Стаб.') %}
      <tr>
        <td data-val="{{ p.name }}" style="font-size:0.83rem;"><strong>{{ p.name }}</strong></td>
        <td data-val="{{ p.segment }}" style="color:#666;font-size:0.8rem;">{{ p.segment }}</td>
        <td data-val="{{ p.total_val }}" style="text-align:right;">{{ "{:,}".format(p.total_val) }}</td>
        <td data-val="{{ tp }}" style="text-align:center;">
          <span style="color:{{ trend_color }};font-weight:600;white-space:nowrap;font-size:0.85rem;">
            {{ trend_icon }} {{ trend_label }}
            {% if td != 'stable' and tp != 0 %}<span style="font-size:0.78rem;margin-left:3px;">({{ '+' if tp > 0 else '' }}{{ tp }}%)</span>{% endif %}
          </span>
        </td>
        <td data-val="{{ er if er is not none else -1 }}" style="text-align:right;font-weight:600;">
          {% if er is not none %}{{ er }}%{% else %}—{% endif %}
        </td>
        <td data-val="{{ etp if etp is not none else 0 }}" style="text-align:center;">
          <span style="color:{{ et_color }};font-weight:600;white-space:nowrap;font-size:0.85rem;">
            {{ et_icon }} {{ et_label }}
            {% if etd != 'stable' and etp is not none %}<span style="font-size:0.78rem;margin-left:3px;">({{ '+' if etp > 0 else '' }}{{ etp }}%)</span>{% endif %}
          </span>
        </td>
      </tr>
      {% endfor %}
      </tbody>
    </table>
    <script>
    (function() {
      var TID = "{{ tbl_id }}";
      var ss = { col: null, dir: null };
      function upd(tbl, col, dir) {
        tbl.querySelectorAll("thead th[data-col] .si").forEach(function(s) {
          var c = parseInt(s.parentNode.getAttribute("data-col"));
          s.textContent = c === col ? (dir === "desc" ? " ▼" : " ▲") : " ⇅";
          s.style.color  = c === col ? "#4f8ef7" : "#bbb";
        });
      }
      function srt(tbl, col, dir) {
        var tbody = tbl.querySelector("tbody");
        var rows = Array.from(tbody.querySelectorAll("tr"));
        rows.sort(function(a, b) {
          var av = a.cells[col] ? (a.cells[col].getAttribute("data-val") || "") : "";
          var bv = b.cells[col] ? (b.cells[col].getAttribute("data-val") || "") : "";
          var an = parseFloat(av), bn = parseFloat(bv);
          var cmp = (!isNaN(an) && !isNaN(bn)) ? an - bn : av.localeCompare(bv, "ru");
          return dir === "desc" ? -cmp : cmp;
        });
        rows.forEach(function(r) { tbody.appendChild(r); });
        ss.col = col; ss.dir = dir; upd(tbl, col, dir);
      }
      document.addEventListener("DOMContentLoaded", function() {
        var tbl = document.getElementById(TID);
        if (!tbl) return;
        tbl.querySelectorAll("thead th[data-col]").forEach(function(th) {
          th.addEventListener("click", function() {
            var col = parseInt(th.getAttribute("data-col"));
            srt(tbl, col, (ss.col === col && ss.dir === "asc") ? "desc" : "asc");
          });
        });
        srt(tbl, 2, "desc");
      });
    })();
    </script>
    {% else %}
    <div class="empty-state">Нет данных по каналу</div>
    {% endif %}
    {% endmacro %}

    <h3 style="font-size:0.95rem;font-weight:600;margin:20px 0 10px;color:#444;">По каналам</h3>
    <div class="two-col" style="margin-top:0;">
      <div>
        <h3 style="font-size:0.9rem;font-weight:600;margin-bottom:8px;color:#555;">🖥️ Web (АРМ)</h3>
        {{ ch_table(ch_data.get('Web (АРМ)', []), 'pd_web_tbl') }}
      </div>
      <div>
        <h3 style="font-size:0.9rem;font-weight:600;margin-bottom:8px;color:#555;">📱 iPad (Планшеты)</h3>
        {{ ch_table(ch_data.get('iPad (Планшеты)', []), 'pd_ipad_tbl') }}
      </div>
    </div>
    {% endif %}
  </div>
  {% endif %}

  <!-- Top-20 services -->
  {% if charts.services_bar %}
  <div class="chart-card">{{ charts.services_bar }}</div>
  {% endif %}

  <!-- Channel & Segment breakdown -->
  <div class="two-col">
    {% if charts.channel_bar %}
    <div class="chart-card">{{ charts.channel_bar }}</div>
    {% endif %}
    {% if charts.segment_pie %}
    <div class="chart-card">{{ charts.segment_pie }}</div>
    {% endif %}
  </div>

  <!-- Status Screen 55558 -->
  {% if ss and ss.total_val > 0 %}
  <div class="section">
    <h2>🖥️ Статусный экран (metric_id=55558) — итого val: {{ "{:,}".format(ss.total_val) }}</h2>
    <div class="two-col" style="margin-bottom:0;">
      {% if charts.status_pie %}
      <div>{{ charts.status_pie }}</div>
      {% endif %}
      <div>
        <table class="stat-table">
          <tr><th>Тип (lvl_4)</th><th>Σ val</th><th>%</th></tr>
          {% set total_ss = ss.total_val %}
          {% for typ, v in ss.total.items() %}
          <tr>
            <td>{{ typ }}</td>
            <td><strong>{{ "{:,}".format(v) }}</strong></td>
            <td>{{ "%.1f"|format(v / total_ss * 100) if total_ss > 0 else 0 }}%</td>
          </tr>
          {% endfor %}
        </table>
      </div>
    </div>
  </div>
  {% endif %}


  <!-- Trends -->
  <div class="section">
    <h2>📈 Тренды по метрикам</h2>
    {% if charts.weekly_channel or charts.weekly_block %}
    <div class="two-col" style="margin-bottom:16px;">
      {% if charts.weekly_channel %}<div>{{ charts.weekly_channel }}</div>{% endif %}
      {% if charts.weekly_block  %}<div>{{ charts.weekly_block  }}</div>{% endif %}
    </div>
    {% endif %}
    {% if charts.trend_lines %}
    {{ charts.trend_lines }}
    {% endif %}
    {% set per_metric = trends.get("per_metric", {}) %}
    {% if per_metric %}
    {% for metric, t in per_metric.items() %}
    <div class="trend-row">
      <div style="font-size:1.1rem;">
        {{ "📈" if t.direction == "growing" else ("📉" if t.direction == "declining" else "➡️") }}
      </div>
      <div style="flex:1;">
        <strong>{{ metric }}</strong>
        <span class="badge {{ 'badge-red' if t.direction == 'declining' else ('badge-green' if t.direction == 'growing' else 'badge-blue') }}"
              style="margin-left:8px;">{{ t.direction }}</span>
      </div>
      <div style="font-size:0.84rem;color:#555;">{{ t.description }}</div>
    </div>
    {% endfor %}
    {% else %}
    <div class="empty-state">Нет данных</div>
    {% endif %}
  </div>

  <!-- Val by date summary -->
  <div class="section">
    <h2>📋 Суммарный val по дням и метрикам</h2>
    <div class="two-col" style="margin-bottom:0;">
      <div>
        <table class="stat-table">
          <tr><th>Дата</th><th>Σ val</th></tr>
          {% for date, v in summary.val_by_date.items() %}
          <tr><td>{{ date }}</td><td><strong>{{ "{:,}".format(v) }}</strong></td></tr>
          {% endfor %}
        </table>
      </div>
      <div>
        <table class="stat-table">
          <tr><th>Метрика</th><th>Σ val</th></tr>
          {% for name, v in summary.val_by_metric.items() %}
          <tr><td>{{ name }}</td><td><strong>{{ "{:,}".format(v) }}</strong></td></tr>
          {% endfor %}
        </table>
      </div>
    </div>
  </div>

</div>

<div class="footer">
  СБОЛ.про Analyzer · Сгенерировано {{ generated_at }}
</div>

</body>
</html>
"""
