import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime
from pathlib import Path
from jinja2 import Template


def generate_report(df: pd.DataFrame, summary: dict, results: dict, output_path: str) -> str:
    """Generate a full HTML report for СБОЛ.про metrics format."""
    charts = _build_charts(df, results)
    html = _render_html(summary, results, charts)
    Path(output_path).write_text(html, encoding="utf-8")
    return output_path


# ─── Chart builders ──────────────────────────────────────────────────────────

def _build_charts(df: pd.DataFrame, results: dict) -> dict:
    charts = {}
    COLORS = px.colors.qualitative.Set2
    COLORS_SEQ = px.colors.sequential.Blues

    # 1. Stacked bar: total val by day, stacked by metric_name
    trends = results.get("trends", {})
    ts_by_metric = trends.get("timeseries", {})
    if ts_by_metric:
        fig = go.Figure()
        for i, (metric, records) in enumerate(ts_by_metric.items()):
            ts_df = pd.DataFrame(records)
            fig.add_trace(go.Bar(
                x=ts_df["date"].astype(str),
                y=ts_df["val"],
                name=metric,
                marker_color=COLORS[i % len(COLORS)],
            ))
        fig.update_layout(
            barmode="stack",
            title="Динамика ошибок по дням",
            height=380,
            legend=dict(orientation="h", y=-0.2),
            margin=dict(l=40, r=20, t=50, b=80),
            plot_bgcolor="#fafafa",
            paper_bgcolor="#ffffff",
            xaxis_title="Дата",
            yaxis_title="Кол-во событий (val)",
        )
        charts["timeline"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 2. WoW deviations — top 15 horizontal bar
    wow = results.get("wow", [])
    if wow:
        top15 = wow[:15]
        labels = [
            f"{r.get('metric_name','')[:20]} / {r.get('segment_name', r.get('lvl_1',''))} / {r.get('lvl_2','')[:20]}"
            for r in top15
        ]
        pcts = [r["pct"] for r in top15]
        colors = ["#e74c3c" if p > 0 else "#3498db" for p in pcts]
        fig = go.Figure(go.Bar(
            y=labels[::-1],
            x=pcts[::-1],
            orientation="h",
            marker_color=colors[::-1],
            text=[f"{p:+.0f}%" for p in pcts[::-1]],
            textposition="outside",
        ))
        fig.update_layout(
            title=f"WoW отклонения (топ-15) — {top15[0]['date_from']} vs {top15[0]['date_to']}",
            height=max(320, len(top15) * 26),
            margin=dict(l=250, r=80, t=50, b=20),
            plot_bgcolor="#fafafa",
            paper_bgcolor="#ffffff",
            xaxis_title="% изменение",
        )
        charts["wow_bar"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 3. DoD deviations — top 15 horizontal bar
    dod = results.get("dod", [])
    if dod:
        top15 = dod[:15]
        labels = [
            f"{r.get('metric_name','')[:20]} / {r.get('segment_name', r.get('lvl_1',''))} / {r.get('lvl_2','')[:20]}"
            for r in top15
        ]
        pcts = [r["pct"] for r in top15]
        colors = ["#e74c3c" if p > 0 else "#3498db" for p in pcts]
        fig = go.Figure(go.Bar(
            y=labels[::-1],
            x=pcts[::-1],
            orientation="h",
            marker_color=colors[::-1],
            text=[f"{p:+.0f}%" for p in pcts[::-1]],
            textposition="outside",
        ))
        fig.update_layout(
            title=f"DoD отклонения (топ-15) — {top15[0]['date_from']} vs {top15[0]['date_to']}",
            height=max(320, len(top15) * 26),
            margin=dict(l=250, r=80, t=50, b=20),
            plot_bgcolor="#fafafa",
            paper_bgcolor="#ffffff",
            xaxis_title="% изменение",
        )
        charts["dod_bar"] = fig.to_html(full_html=False, include_plotlyjs=False)

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
        fig = go.Figure()
        for i, (metric, records) in enumerate(ts_by_metric.items()):
            ts_df = pd.DataFrame(records)
            fig.add_trace(go.Scatter(
                x=ts_df["date"].astype(str),
                y=ts_df["val"],
                name=metric,
                mode="lines+markers",
                line=dict(color=COLORS[i % len(COLORS)], width=2),
            ))
        fig.update_layout(
            title="Тренды по типам ошибок",
            height=340,
            legend=dict(orientation="h", y=-0.2),
            margin=dict(l=40, r=20, t=50, b=80),
            plot_bgcolor="#fafafa",
            paper_bgcolor="#ffffff",
        )
        charts["trend_lines"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 9. Product drill-down: dropdown selector → bar chart by block_type or lvl_4 per day
    pd_data = results.get("product_dynamics", {})
    block_matrix = pd_data.get("block_matrix", {})
    lvl4_matrix = pd_data.get("lvl4_matrix", {})
    channel_matrix = pd_data.get("channel_matrix", {})
    dates_pd = pd_data.get("dates", [])
    products_meta = pd_data.get("products", [])

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
        if use_lvl4:
            primary_keys = ["Ошибка", "Ожидание", "Информирование", "Успех"]
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

            # Channel lines (secondary axis)
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

        # Build dropdown buttons
        for i, pm in enumerate(products_meta):
            vis = [False] * total_traces
            for vi in visibility_map[i]:
                vis[vi] = True
            dod_str = f"DoD {pm['dod_pct']:+.1f}%" if pm.get("dod_pct") is not None else "DoD —"
            wow_str = f"WoW {pm['wow_pct']:+.1f}%" if pm.get("wow_pct") is not None else "WoW —"
            buttons.append(dict(
                label=f"{pm['name']}  [{dod_str} / {wow_str}]",
                method="update",
                args=[
                    {"visible": vis, "showlegend": vis},
                    {"title": f"📦 {pm['name']} — по {group_label.lower()}  |  {dod_str}  ·  {wow_str}  |  Σ {pm['total_val']:,}"},
                ],
            ))

        fig = go.Figure(data=all_traces)
        first = products_meta[0]
        dod_str0 = f"DoD {first['dod_pct']:+.1f}%" if first.get("dod_pct") is not None else "DoD —"
        wow_str0 = f"WoW {first['wow_pct']:+.1f}%" if first.get("wow_pct") is not None else "WoW —"
        fig.update_layout(
            title=f"📦 {first['name']} — по {group_label.lower()}  |  {dod_str0}  ·  {wow_str0}  |  Σ {first['total_val']:,}",
            barmode="stack",
            height=440,
            margin=dict(l=40, r=40, t=90, b=60),
            plot_bgcolor="#fafafa",
            paper_bgcolor="#ffffff",
            xaxis_title="Дата",
            yaxis=dict(title=f"Кол-во событий ({group_label})", side="left"),
            yaxis2=dict(title="Кол-во событий (каналы)", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", y=-0.18),
            updatemenus=[dict(
                buttons=buttons,
                direction="down",
                showactive=True,
                x=0.0, xanchor="left",
                y=1.18, yanchor="top",
                bgcolor="#ffffff",
                bordercolor="#d0d7de",
                font=dict(size=12),
            )],
        )
        charts["product_drill"] = fig.to_html(full_html=False, include_plotlyjs=False)

    return charts


# ─── HTML rendering ──────────────────────────────────────────────────────────

def _render_html(summary: dict, results: dict, charts: dict) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    anomalies = results.get("anomalies", [])
    wow = results.get("wow", [])
    dod = results.get("dod", [])
    trends = results.get("trends", {})
    top_services = results.get("top_services", [])
    ss = results.get("status_screen", {})

    pd_data = results.get("product_dynamics", {})

    template = Template(HTML_TEMPLATE)
    return template.render(
        generated_at=generated_at,
        summary=summary,
        anomalies=anomalies,
        wow=wow,
        dod=dod,
        trends=trends,
        top_services=top_services,
        ss=ss,
        charts=charts,
        pd_data=pd_data,
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
      <div class="value" style="color: #e74c3c;">{{ anomalies | length }}</div>
      <div class="label">Аномалий (IQR×3)</div>
    </div>
    <div class="kpi-card">
      <div class="value" style="color: #f39c12;">{{ wow | length }}</div>
      <div class="label">WoW отклонений</div>
    </div>
    <div class="kpi-card">
      <div class="value" style="color: #e67e22;">{{ dod | length }}</div>
      <div class="label">DoD отклонений</div>
    </div>
  </div>

  <!-- Timeline chart -->
  {% if charts.timeline %}
  <div class="chart-card">{{ charts.timeline }}</div>
  {% endif %}

  <!-- WoW & DoD side by side -->
  <div class="two-col">
    <!-- WoW -->
    <div class="section">
      <h2>📅 WoW отклонения (неделя к неделе) — {{ wow | length }}</h2>
      {% if charts.wow_bar %}
        {{ charts.wow_bar }}
      {% elif wow %}
        {% for d in wow[:15] %}
        <div class="deviation-row {{ 'dev-spike' if d.direction == 'spike' else 'dev-drop' }}">
          <strong>{{ d.get('metric_name','') }}</strong> /
          {{ d.get('segment_name', d.get('lvl_1','')) }} / {{ d.get('lvl_2','') }}
          <br>
          <span style="font-size:0.78rem;color:#888;">{{ d.date_from }} → {{ d.date_to }} · блок: {{ d.get('block_type','') }}</span>
          <br>
          {{ d.val_prev }} → <strong>{{ d.val_curr }}</strong>
          <span style="color:{{ '#e74c3c' if d.direction=='spike' else '#3498db' }};font-weight:700;">
            {{ '+' if d.pct > 0 else '' }}{{ d.pct }}%
          </span>
        </div>
        {% endfor %}
        {% if wow | length > 15 %}
        <div style="color:#888;font-size:0.82rem;padding:8px;">... и ещё {{ wow | length - 15 }}</div>
        {% endif %}
      {% else %}
        <div class="empty-state">✅ WoW отклонений нет (нет данных за -7 дней или ниже порога)</div>
      {% endif %}
    </div>

    <!-- DoD -->
    <div class="section">
      <h2>📆 DoD отклонения (день к дню) — {{ dod | length }}</h2>
      {% if charts.dod_bar %}
        {{ charts.dod_bar }}
      {% elif dod %}
        {% for d in dod[:15] %}
        <div class="deviation-row {{ 'dev-spike' if d.direction == 'spike' else 'dev-drop' }}">
          <strong>{{ d.get('metric_name','') }}</strong> /
          {{ d.get('segment_name', d.get('lvl_1','')) }} / {{ d.get('lvl_2','') }}
          <br>
          <span style="font-size:0.78rem;color:#888;">{{ d.date_from }} → {{ d.date_to }} · блок: {{ d.get('block_type','') }}</span>
          <br>
          {{ d.val_prev }} → <strong>{{ d.val_curr }}</strong>
          <span style="color:{{ '#e74c3c' if d.direction=='spike' else '#3498db' }};font-weight:700;">
            {{ '+' if d.pct > 0 else '' }}{{ d.pct }}%
          </span>
        </div>
        {% endfor %}
        {% if dod | length > 15 %}
        <div style="color:#888;font-size:0.82rem;padding:8px;">... и ещё {{ dod | length - 15 }}</div>
        {% endif %}
      {% else %}
        <div class="empty-state">✅ DoD отклонений нет (ниже порога >50%)</div>
      {% endif %}
    </div>
  </div>

  <!-- Product drill-down: dropdown → bar by block_type + channel lines -->
  {% if charts.product_drill %}
  <div class="section">
    <h2>🗓️ Динамика по продукту день за днём</h2>
    <p style="color:#888;font-size:0.85rem;margin-bottom:12px;">
      Выберите продукт из списка — увидите динамику ошибок по блокам (боевой / пилотный / резервный) и каналам.
    </p>
    {{ charts.product_drill }}
    {% if pd_data.products %}
    <div style="margin-top:16px;overflow-x:auto;">
      <table class="stat-table">
        <thead>
          <tr>
            <th>Продукт</th>
            <th>Сегмент</th>
            <th>Тип ошибки</th>
            <th>Σ val</th>
            <th>DoD ({{ pd_data.last_date }})</th>
            <th>WoW ({{ pd_data.last_date }})</th>
          </tr>
        </thead>
        <tbody>
        {% for p in pd_data.products %}
        {% set dod_v = pd_data.dod_pct.get(p.name) %}
        {% set wow_v = pd_data.wow_pct.get(p.name) %}
        <tr>
          <td><strong>{{ p.name }}</strong></td>
          <td>{{ p.segment }}</td>
          <td style="color:#888;font-size:0.82rem;">{{ p.metric_name }}</td>
          <td>{{ "{:,}".format(p.total_val) }}</td>
          <td>
            {% if dod_v is not none %}
              <span style="color:{{ '#e74c3c' if dod_v > 0 else '#27ae60' }};font-weight:600;">
                {{ '+' if dod_v > 0 else '' }}{{ dod_v }}%
              </span>
            {% else %}<span style="color:#bbb">—</span>{% endif %}
          </td>
          <td>
            {% if wow_v is not none %}
              <span style="color:{{ '#e74c3c' if wow_v > 0 else '#27ae60' }};font-weight:600;">
                {{ '+' if wow_v > 0 else '' }}{{ wow_v }}%
              </span>
            {% else %}<span style="color:#bbb">—</span>{% endif %}
          </td>
        </tr>
        {% endfor %}
        </tbody>
      </table>
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

  <!-- Anomalies table -->
  <div class="section">
    <h2>⚡ Аномальные значения (IQR×3) — {{ anomalies | length }}</h2>
    {% if anomalies %}
    <table class="stat-table">
      <tr>
        <th>Дата</th><th>Метрика</th><th>Канал</th><th>Сегмент</th>
        <th>Продукт</th><th>Блок</th><th>val</th><th>Порог</th><th>Z</th><th>Уровень</th>
      </tr>
      {% for a in anomalies[:30] %}
      <tr>
        <td>{{ a.timestamp }}</td>
        <td>{{ a.metric_name }}</td>
        <td>{{ a.channel_name }}</td>
        <td>{{ a.segment_name }}</td>
        <td>{{ a.lvl_2 }}</td>
        <td>{{ a.block_type }}</td>
        <td><strong>{{ "{:,}".format(a.val) }}</strong></td>
        <td>{{ "{:,.0f}".format(a.threshold) }}</td>
        <td>{{ a.zscore }}</td>
        <td>
          <span class="badge {{ 'badge-red' if a.severity == 'high' else 'badge-orange' }}">
            {{ a.severity }}
          </span>
        </td>
      </tr>
      {% endfor %}
    </table>
    {% if anomalies | length > 30 %}
    <div style="color:#888;font-size:0.82rem;padding:10px;">... и ещё {{ anomalies | length - 30 }} записей</div>
    {% endif %}
    {% else %}
    <div class="empty-state">✅ Аномалий не обнаружено</div>
    {% endif %}
  </div>

  <!-- Trends -->
  <div class="section">
    <h2>📈 Тренды по метрикам</h2>
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
