import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime
from pathlib import Path
from jinja2 import Template


def generate_report(df: pd.DataFrame, summary: dict, results: dict, output_path: str) -> str:
    """Generate a full HTML report for the metrics / platform-driver format."""
    charts = _build_charts(df, results)
    html = _render_html(summary, results, charts)
    Path(output_path).write_text(html, encoding="utf-8")
    return output_path


# ---------------------------------------------------------------------------
# Chart builders
# ---------------------------------------------------------------------------

def _build_charts(df: pd.DataFrame, results: dict) -> dict:
    charts = {}
    COLORS = px.colors.qualitative.Set2

    # 1. Daily total val by metric (stacked bar)
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
            barmode="group",
            title="Суммарный val по дням и метрикам",
            height=360,
            legend=dict(orientation="h", y=-0.2),
            margin=dict(l=40, r=20, t=50, b=70),
            plot_bgcolor="#fafafa",
            paper_bgcolor="#ffffff",
            xaxis_title="Дата",
            yaxis_title="Сумма val",
        )
        charts["timeline"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 2. Val distribution by category (lvl_1) — pie
    metric_stats = results.get("metric_stats", {})
    by_cat = metric_stats.get("by_category", {})
    if by_cat:
        fig = go.Figure(go.Pie(
            labels=list(by_cat.keys()),
            values=list(by_cat.values()),
            hole=0.4,
            textinfo="label+percent",
            marker=dict(colors=COLORS),
        ))
        fig.update_layout(
            title="Распределение val по категориям (lvl_1)",
            height=360,
            margin=dict(l=20, r=20, t=50, b=20),
        )
        charts["cat_pie"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 3. Val by platform — bar
    by_platform = metric_stats.get("by_platform", {})
    if by_platform:
        labels = list(by_platform.keys())
        values = list(by_platform.values())
        fig = go.Figure(go.Bar(
            x=labels,
            y=values,
            marker_color=COLORS[:len(labels)],
            text=[f"{v:,}" for v in values],
            textposition="outside",
        ))
        fig.update_layout(
            title="Суммарный val по платформам",
            height=320,
            margin=dict(l=40, r=20, t=50, b=60),
            plot_bgcolor="#fafafa",
            paper_bgcolor="#ffffff",
        )
        charts["platform_bar"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 4. Top-15 environments by total val (horizontal bar)
    env_stats = results.get("environment_stats", {})
    top_envs = env_stats.get("top_environments", {})
    if top_envs:
        envs = list(top_envs.keys())[:15]
        vals = [top_envs[e] for e in envs]
        fig = go.Figure(go.Bar(
            y=envs[::-1],
            x=vals[::-1],
            orientation="h",
            marker_color="#4f8ef7",
            text=[f"{v:,}" for v in vals[::-1]],
            textposition="outside",
        ))
        fig.update_layout(
            title="Топ окружений по суммарному val",
            height=420,
            margin=dict(l=120, r=80, t=50, b=20),
            plot_bgcolor="#fafafa",
            paper_bgcolor="#ffffff",
        )
        charts["env_bar"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 5. Top-15 workflows by total val (horizontal bar)
    wf_stats = results.get("workflow_stats", {})
    top_wf = wf_stats.get("top_workflows", [])
    if top_wf:
        labels = [f"{r['lvl_1']} / {r['lvl_2']}" for r in top_wf[:15]]
        vals = [r["val"] for r in top_wf[:15]]
        fig = go.Figure(go.Bar(
            y=labels[::-1],
            x=vals[::-1],
            orientation="h",
            marker_color="#27ae60",
            text=[f"{v:,}" for v in vals[::-1]],
            textposition="outside",
        ))
        fig.update_layout(
            title="Топ workflow по суммарному val",
            height=440,
            margin=dict(l=200, r=80, t=50, b=20),
            plot_bgcolor="#fafafa",
            paper_bgcolor="#ffffff",
        )
        charts["wf_bar"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 6. Day-over-day deviations scatter (% change)
    deviations = results.get("deviations", [])
    if deviations:
        dev_df = pd.DataFrame(deviations[:50])
        colors = ["#e74c3c" if d == "spike" else "#3498db" for d in dev_df["direction"]]
        labels = dev_df["workflow"] + " / " + dev_df["environment"]
        fig = go.Figure(go.Bar(
            x=dev_df["pct"],
            y=labels,
            orientation="h",
            marker_color=colors,
            text=[f"{p:+.0f}%" for p in dev_df["pct"]],
            textposition="outside",
        ))
        fig.update_layout(
            title="Топ отклонений день к дню (% изменение val)",
            height=max(350, len(dev_df) * 22),
            margin=dict(l=220, r=80, t=50, b=20),
            plot_bgcolor="#fafafa",
            paper_bgcolor="#ffffff",
            xaxis_title="% изменение",
        )
        charts["deviations_bar"] = fig.to_html(full_html=False, include_plotlyjs=False)

    return charts


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

def _render_html(summary: dict, results: dict, charts: dict) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    anomalies = results.get("anomalies", [])
    deviations = results.get("deviations", [])
    wf_anom = results.get("workflow_anomalies", {})
    trends = results.get("trends", {})

    template = Template(HTML_TEMPLATE)
    return template.render(
        generated_at=generated_at,
        summary=summary,
        anomalies=anomalies,
        deviations=deviations,
        wf_anom=wf_anom,
        trends=trends,
        charts=charts,
    )


# ---------------------------------------------------------------------------
# HTML Template
# ---------------------------------------------------------------------------

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Platform Driver — Anomaly Report</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
           background: #f0f2f5; color: #1a1a2e; line-height: 1.6; }
    .header { background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
              color: white; padding: 40px; }
    .header h1 { font-size: 2rem; font-weight: 700; margin-bottom: 8px; }
    .header .meta { opacity: 0.7; font-size: 0.9rem; }
    .container { max-width: 1400px; margin: 0 auto; padding: 24px; }
    .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
                gap: 16px; margin-bottom: 24px; }
    .kpi-card { background: white; border-radius: 12px; padding: 20px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.08); text-align: center; }
    .kpi-card .value { font-size: 1.8rem; font-weight: 700; color: #4f8ef7; }
    .kpi-card .label { font-size: 0.78rem; color: #888; text-transform: uppercase;
                       letter-spacing: 0.5px; margin-top: 4px; }
    .section { background: white; border-radius: 12px; padding: 24px;
               margin-bottom: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
    .section h2 { font-size: 1.15rem; font-weight: 600; margin-bottom: 16px;
                  padding-bottom: 10px; border-bottom: 2px solid #f0f2f5; }
    .charts-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
    .chart-card { background: white; border-radius: 12px; padding: 16px;
                  box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
    .chart-full { grid-column: 1 / -1; }
    .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
    .alert-list { list-style: none; }
    .alert-item { padding: 10px 14px; border-radius: 8px; margin-bottom: 8px; border-left: 4px solid; }
    .alert-high   { background: #fff5f5; border-color: #e74c3c; }
    .alert-medium { background: #fffbf0; border-color: #f39c12; }
    .alert-info   { background: #f0f8ff; border-color: #3498db; }
    .alert-item .ts   { font-size: 0.78rem; color: #888; }
    .alert-item .desc { font-weight: 500; margin-top: 2px; font-size: 0.88rem; }
    .badge { display: inline-block; padding: 2px 10px; border-radius: 12px;
             font-size: 0.72rem; font-weight: 600; }
    .badge-red    { background: #ffe0e0; color: #c0392b; }
    .badge-orange { background: #fff3cd; color: #d68910; }
    .badge-green  { background: #e0f7ea; color: #1e8449; }
    .badge-blue   { background: #e0f0ff; color: #2471a3; }
    .stat-table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
    .stat-table th { text-align: left; padding: 8px 12px; background: #f8f9fa;
                     font-size: 0.78rem; text-transform: uppercase; color: #888; }
    .stat-table td { padding: 8px 12px; border-bottom: 1px solid #f0f2f5; }
    .stat-table tr:last-child td { border-bottom: none; }
    .trend-row { display: flex; align-items: center; gap: 12px; padding: 8px 0;
                 border-bottom: 1px solid #f0f2f5; }
    .trend-row:last-child { border-bottom: none; }
    .trend-badge { font-size: 1rem; font-weight: 700; min-width: 24px; }
    .empty-state { text-align: center; padding: 24px; color: #aaa; }
    .footer { text-align: center; padding: 30px; color: #aaa; font-size: 0.85rem; }
    @media (max-width: 768px) {
      .charts-grid, .two-col { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>

<div class="header">
  <h1>📊 Platform Driver — Anomaly Report</h1>
  <div class="meta">
    Период: {{ summary.date_range.start }} → {{ summary.date_range.end }}
    &nbsp;·&nbsp; Метрики: {{ summary.metrics | join(", ") }}
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
      <div class="value">{{ summary.unique_metrics }}</div>
      <div class="label">Метрик</div>
    </div>
    <div class="kpi-card">
      <div class="value">{{ summary.unique_workflows }}</div>
      <div class="label">Workflow</div>
    </div>
    <div class="kpi-card">
      <div class="value">{{ summary.unique_environments }}</div>
      <div class="label">Окружений</div>
    </div>
    <div class="kpi-card">
      <div class="value" style="color: #e74c3c;">{{ anomalies | length }}</div>
      <div class="label">Выбросов val</div>
    </div>
    <div class="kpi-card">
      <div class="value" style="color: #f39c12;">{{ deviations | length }}</div>
      <div class="label">Отклонений день к дню</div>
    </div>
  </div>

  <!-- Timeline chart -->
  {% if charts.timeline %}
  <div class="chart-card" style="margin-bottom: 20px;">
    {{ charts.timeline }}
  </div>
  {% endif %}

  <!-- Anomalies + Deviations -->
  <div class="two-col">
    <div class="section">
      <h2>⚡ Выбросы val ({{ anomalies | length }})</h2>
      {% if anomalies %}
      <ul class="alert-list">
        {% for a in anomalies[:10] %}
        <li class="alert-item {{ 'alert-high' if a.severity == 'high' else 'alert-medium' }}">
          <div class="ts">{{ a.timestamp }} · {{ a.metric_name }}</div>
          <div class="desc">{{ a.description }}</div>
          <span class="badge {{ 'badge-red' if a.severity == 'high' else 'badge-orange' }}">
            {{ a.severity }}
          </span>
          <span class="badge badge-blue">z={{ a.zscore }}</span>
        </li>
        {% endfor %}
        {% if anomalies | length > 10 %}
        <li style="padding: 8px; color: #888;">... и ещё {{ anomalies | length - 10 }} выбросов</li>
        {% endif %}
      </ul>
      {% else %}
      <div class="empty-state">✅ Выбросов не обнаружено</div>
      {% endif %}
    </div>

    <div class="section">
      <h2>📐 Отклонения день к дню ({{ deviations | length }})</h2>
      {% if deviations %}
      <ul class="alert-list">
        {% for d in deviations[:10] %}
        <li class="alert-item {{ 'alert-high' if d.pct | abs > 200 else 'alert-medium' }}">
          <div class="ts">{{ d.date_from }} → {{ d.date_to }} · {{ d.metric_name }}</div>
          <div class="desc">
            {{ d.category }} / {{ d.workflow }} / {{ d.environment }}:
            {{ d.val_prev }} → <strong>{{ d.val_curr }}</strong>
            (<span style="color: {{ '#e74c3c' if d.direction == 'spike' else '#3498db' }}">
              {{ '+' if d.pct > 0 else '' }}{{ d.pct }}%
            </span>)
          </div>
        </li>
        {% endfor %}
        {% if deviations | length > 10 %}
        <li style="padding: 8px; color: #888;">... и ещё {{ deviations | length - 10 }}</li>
        {% endif %}
      </ul>
      {% else %}
      <div class="empty-state">✅ Значимых отклонений нет</div>
      {% endif %}
    </div>
  </div>

  <!-- Trend per metric -->
  <div class="section">
    <h2>📈 Тренды по метрикам</h2>
    {% set per_metric = trends.get("per_metric", {}) %}
    {% if per_metric %}
    {% for metric, t in per_metric.items() %}
    <div class="trend-row">
      <div class="trend-badge">
        {{ "📈" if t.direction == "growing" else ("📉" if t.direction == "declining" else "➡️") }}
      </div>
      <div style="flex: 1;">
        <strong>{{ metric }}</strong>
        <span class="badge {{ 'badge-red' if t.direction == 'declining' else ('badge-green' if t.direction == 'growing' else 'badge-blue') }}"
              style="margin-left: 8px;">{{ t.direction }}</span>
      </div>
      <div style="font-size: 0.85rem; color: #555;">{{ t.description }}</div>
    </div>
    {% endfor %}
    {% else %}
    <div class="empty-state">Нет данных</div>
    {% endif %}
  </div>

  <!-- Charts -->
  <div class="charts-grid">
    {% if charts.cat_pie %}
    <div class="chart-card">{{ charts.cat_pie }}</div>
    {% endif %}
    {% if charts.platform_bar %}
    <div class="chart-card">{{ charts.platform_bar }}</div>
    {% endif %}
    {% if charts.env_bar %}
    <div class="chart-card chart-full">{{ charts.env_bar }}</div>
    {% endif %}
    {% if charts.wf_bar %}
    <div class="chart-card chart-full">{{ charts.wf_bar }}</div>
    {% endif %}
    {% if charts.deviations_bar %}
    <div class="chart-card chart-full">{{ charts.deviations_bar }}</div>
    {% endif %}
  </div>

  <!-- Anomalous Workflows -->
  <div class="two-col">
    <div class="section">
      <h2>🔍 Аномальные workflow ({{ wf_anom.get("anomalous_count", 0) }} из {{ wf_anom.get("total_workflows", 0) }})</h2>
      {% set wf_list = wf_anom.get("anomalous_workflows", []) %}
      {% if wf_list %}
      <table class="stat-table">
        <tr>
          <th>Метрика</th><th>Кат.</th><th>Workflow</th>
          <th>Σ val</th><th>Z-score</th>
        </tr>
        {% for w in wf_list[:12] %}
        <tr>
          <td>{{ w.metric_name }}</td>
          <td>{{ w.category }}</td>
          <td><strong>{{ w.workflow }}</strong></td>
          <td>{{ "{:,}".format(w.total_val) }}</td>
          <td>
            <span class="badge {{ 'badge-red' if w.zscore | abs > 3.5 else 'badge-orange' }}">
              {{ w.zscore }}
            </span>
          </td>
        </tr>
        {% endfor %}
      </table>
      {% else %}
      <div class="empty-state">✅ Аномальных workflow нет</div>
      {% endif %}
    </div>

    <!-- Val by metric summary -->
    <div class="section">
      <h2>📋 Суммарный val по метрикам</h2>
      {% set by_metric = results_metric_by_metric | default({}) %}
      <table class="stat-table">
        <tr><th>Метрика</th><th>Σ val</th></tr>
        {% for name, v in summary.val_by_metric.items() %}
        <tr>
          <td>{{ name }}</td>
          <td><strong>{{ "{:,}".format(v) }}</strong></td>
        </tr>
        {% endfor %}
      </table>
      <div style="margin-top: 16px;">
        <div style="font-size: 0.8rem; color: #888; text-transform: uppercase; margin-bottom: 8px;">По датам</div>
        {% for date, v in summary.val_by_date.items() %}
        <div style="display: flex; justify-content: space-between; padding: 4px 0;
                    border-bottom: 1px solid #f0f2f5; font-size: 0.88rem;">
          <span>{{ date }}</span>
          <strong>{{ "{:,}".format(v) }}</strong>
        </div>
        {% endfor %}
      </div>
    </div>
  </div>

</div>

<div class="footer">
  Platform Driver Analyzer · Сгенерировано {{ generated_at }}
</div>

</body>
</html>
"""
