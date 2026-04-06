import json
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime
from pathlib import Path
from jinja2 import Template


def generate_report(df: pd.DataFrame, summary: dict, results: dict, output_path: str) -> str:
    """Generate a full HTML report."""
    charts = _build_charts(df, results)
    html = _render_html(summary, results, charts)
    Path(output_path).write_text(html, encoding="utf-8")
    return output_path


def _build_charts(df: pd.DataFrame, results: dict) -> dict:
    charts = {}

    # 1. Traffic over time with anomaly markers
    trend = results.get("trends", {})
    ts_data = trend.get("timeseries", [])
    if ts_data:
        ts_df = pd.DataFrame(ts_data)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=ts_df["timestamp"], y=ts_df["count"],
            mode="lines", name="Events", line=dict(color="#4f8ef7", width=2)
        ))
        fig.add_trace(go.Scatter(
            x=ts_df["timestamp"], y=ts_df["ma"],
            mode="lines", name="Moving avg",
            line=dict(color="#f7a24f", width=2, dash="dash")
        ))

        # Add anomaly points
        anomalies = results.get("anomalies", [])
        if anomalies:
            ax = [a["timestamp"] for a in anomalies]
            ay = [a["count"] for a in anomalies]
            colors = ["red" if a["severity"] == "high" else "orange" for a in anomalies]
            fig.add_trace(go.Scatter(
                x=ax, y=ay, mode="markers", name="Anomalies",
                marker=dict(color=colors, size=12, symbol="x"),
            ))

        fig.update_layout(
            title="Traffic Timeline", height=380,
            legend=dict(orientation="h", y=-0.15),
            margin=dict(l=40, r=20, t=50, b=60),
            plot_bgcolor="#fafafa", paper_bgcolor="#ffffff",
        )
        charts["timeline"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 2. Event type distribution (pie)
    event_dist = results.get("event_stats", {}).get("distribution", {})
    if event_dist:
        fig = go.Figure(go.Pie(
            labels=list(event_dist.keys()), values=list(event_dist.values()),
            hole=0.4, textinfo="label+percent",
            marker=dict(colors=px.colors.qualitative.Set2)
        ))
        fig.update_layout(title="Event Types", height=360, margin=dict(l=20, r=20, t=50, b=20))
        charts["event_pie"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 3. Top pages (bar)
    page_stats = results.get("page_stats", {})
    top_pages = page_stats.get("top_pages", {})
    if top_pages:
        pages = list(top_pages.keys())[:10]
        counts = [top_pages[p] for p in pages]
        fig = go.Figure(go.Bar(
            y=pages[::-1], x=counts[::-1], orientation="h",
            marker_color="#4f8ef7", text=counts[::-1], textposition="outside"
        ))
        fig.update_layout(
            title="Top Pages", height=380,
            margin=dict(l=150, r=60, t=50, b=20),
            plot_bgcolor="#fafafa", paper_bgcolor="#ffffff",
        )
        charts["top_pages"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 4. Hourly heatmap / bar
    hourly = df.groupby(df["timestamp"].dt.hour).size().reindex(range(24), fill_value=0)
    fig = go.Figure(go.Bar(
        x=list(range(24)), y=hourly.values,
        marker_color=["#e74c3c" if h in trend.get("peak_hours", []) else "#4f8ef7" for h in range(24)],
        text=hourly.values, textposition="outside",
    ))
    fig.update_layout(
        title="Events by Hour of Day (red = peak)",
        xaxis=dict(tickvals=list(range(24)), ticktext=[f"{h:02d}:00" for h in range(24)]),
        height=330, margin=dict(l=40, r=20, t=50, b=60),
        plot_bgcolor="#fafafa", paper_bgcolor="#ffffff",
    )
    charts["hourly"] = fig.to_html(full_html=False, include_plotlyjs=False)

    # 5. Session depth distribution
    sess = results.get("session_stats", {})
    if df["session_id"].nunique() > 1:
        sess_events = df.groupby("session_id").size()
        fig = go.Figure(go.Histogram(
            x=sess_events.values, nbinsx=30,
            marker_color="#27ae60", opacity=0.8,
        ))
        fig.update_layout(
            title="Session Depth (events per session)",
            xaxis_title="Events", yaxis_title="Sessions",
            height=320, margin=dict(l=40, r=20, t=50, b=50),
            plot_bgcolor="#fafafa", paper_bgcolor="#ffffff",
        )
        charts["session_hist"] = fig.to_html(full_html=False, include_plotlyjs=False)

    return charts


def _render_html(summary: dict, results: dict, charts: dict) -> str:
    trend = results.get("trends", {})
    anomalies = results.get("anomalies", [])
    deviations = results.get("deviations", [])
    user_anom = results.get("user_anomalies", {})
    sess = results.get("session_stats", {})
    page_stats = results.get("page_stats", {})

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    trend_icon = {"growing": "📈", "declining": "📉", "stable": "➡️"}.get(trend.get("direction", ""), "❓")
    trend_color = {"growing": "#27ae60", "declining": "#e74c3c", "stable": "#7f8c8d"}.get(trend.get("direction", ""), "#333")

    template = Template(HTML_TEMPLATE)
    return template.render(
        generated_at=generated_at,
        summary=summary,
        trend=trend,
        trend_icon=trend_icon,
        trend_color=trend_color,
        anomalies=anomalies,
        deviations=deviations,
        user_anom=user_anom,
        sess=sess,
        page_stats=page_stats,
        charts=charts,
    )


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Clickstream Analysis Report</title>
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
  .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
              gap: 16px; margin-bottom: 24px; }
  .kpi-card { background: white; border-radius: 12px; padding: 20px;
              box-shadow: 0 2px 8px rgba(0,0,0,0.08); text-align: center; }
  .kpi-card .value { font-size: 2rem; font-weight: 700; color: #4f8ef7; }
  .kpi-card .label { font-size: 0.8rem; color: #888; text-transform: uppercase;
                     letter-spacing: 0.5px; margin-top: 4px; }
  .section { background: white; border-radius: 12px; padding: 24px; margin-bottom: 20px;
             box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
  .section h2 { font-size: 1.2rem; font-weight: 600; margin-bottom: 16px; padding-bottom: 10px;
                border-bottom: 2px solid #f0f2f5; }
  .charts-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
  .chart-card { background: white; border-radius: 12px; padding: 16px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
  .chart-full { grid-column: 1 / -1; }
  .alert-list { list-style: none; }
  .alert-item { padding: 12px 16px; border-radius: 8px; margin-bottom: 8px;
                border-left: 4px solid; }
  .alert-high { background: #fff5f5; border-color: #e74c3c; }
  .alert-medium { background: #fffbf0; border-color: #f39c12; }
  .alert-info { background: #f0f8ff; border-color: #3498db; }
  .alert-item .time { font-size: 0.8rem; color: #888; }
  .alert-item .desc { font-weight: 500; margin-top: 2px; }
  .trend-badge { display: inline-flex; align-items: center; gap: 8px; padding: 10px 20px;
                 border-radius: 24px; font-weight: 600; font-size: 1.1rem; }
  .stat-table { width: 100%; border-collapse: collapse; }
  .stat-table th { text-align: left; padding: 10px 12px; background: #f8f9fa;
                   font-size: 0.8rem; text-transform: uppercase; color: #888; }
  .stat-table td { padding: 10px 12px; border-bottom: 1px solid #f0f2f5; }
  .stat-table tr:last-child td { border-bottom: none; }
  .badge { display: inline-block; padding: 2px 10px; border-radius: 12px; font-size: 0.75rem; font-weight: 600; }
  .badge-red { background: #ffe0e0; color: #c0392b; }
  .badge-orange { background: #fff3cd; color: #d68910; }
  .badge-green { background: #e0f7ea; color: #1e8449; }
  .badge-blue { background: #e0f0ff; color: #2471a3; }
  .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  @media (max-width: 768px) { .charts-grid, .two-col { grid-template-columns: 1fr; } }
  .empty-state { text-align: center; padding: 30px; color: #aaa; }
  .footer { text-align: center; padding: 30px; color: #aaa; font-size: 0.85rem; }
</style>
</head>
<body>
<div class="header">
  <h1>📊 Clickstream Analysis Report</h1>
  <div class="meta">
    Период: {{ summary.time_range.start[:16] }} → {{ summary.time_range.end[:16] }}
    &nbsp;·&nbsp; Сгенерирован: {{ generated_at }}
  </div>
</div>

<div class="container">

  <!-- KPI Cards -->
  <div class="kpi-grid">
    <div class="kpi-card">
      <div class="value">{{ "{:,}".format(summary.total_events) }}</div>
      <div class="label">Всего событий</div>
    </div>
    <div class="kpi-card">
      <div class="value">{{ "{:,}".format(summary.unique_users) }}</div>
      <div class="label">Уникальных пользователей</div>
    </div>
    <div class="kpi-card">
      <div class="value">{{ "{:,}".format(summary.unique_sessions) }}</div>
      <div class="label">Сессий</div>
    </div>
    <div class="kpi-card">
      <div class="value">{{ summary.unique_pages }}</div>
      <div class="label">Уникальных страниц</div>
    </div>
    <div class="kpi-card">
      <div class="value">{{ summary.time_range.duration_hours }}ч</div>
      <div class="label">Период данных</div>
    </div>
    <div class="kpi-card">
      <div class="value" style="color: {{ trend_color }}">{{ trend_icon }}</div>
      <div class="label">Тренд трафика</div>
    </div>
  </div>

  <!-- Timeline chart (full width) -->
  {% if charts.timeline %}
  <div class="chart-card" style="margin-bottom: 20px;">
    {{ charts.timeline }}
  </div>
  {% endif %}

  <!-- Anomalies & Deviations -->
  <div class="two-col">
    <div class="section">
      <h2>⚡ Аномалии объёма ({{ anomalies|length }})</h2>
      {% if anomalies %}
      <ul class="alert-list">
        {% for a in anomalies %}
        <li class="alert-item {{ 'alert-high' if a.severity == 'high' else 'alert-medium' }}">
          <div class="time">{{ a.timestamp[:16] }}</div>
          <div class="desc">{{ a.description }}</div>
          <span class="badge {{ 'badge-red' if a.severity == 'high' else 'badge-orange' }}">
            {{ a.severity }}
          </span>
        </li>
        {% endfor %}
      </ul>
      {% else %}
      <div class="empty-state">✅ Аномалий не обнаружено</div>
      {% endif %}
    </div>

    <div class="section">
      <h2>📐 Отклонения от тренда ({{ deviations|length }})</h2>
      {% if deviations %}
      <ul class="alert-list">
        {% for d in deviations[:8] %}
        <li class="alert-item {{ 'alert-high' if d.zscore|abs > 3 else 'alert-medium' }}">
          <div class="time">{{ d.timestamp[:16] }}</div>
          <div class="desc">
            {{ d.count }} событий (ожидалось ~{{ d.expected }},
            <strong>{{ '+' if d.deviation_pct > 0 else '' }}{{ d.deviation_pct }}%</strong>)
          </div>
        </li>
        {% endfor %}
        {% if deviations|length > 8 %}
        <li style="padding: 8px; color: #888;">... и ещё {{ deviations|length - 8 }} отклонений</li>
        {% endif %}
      </ul>
      {% else %}
      <div class="empty-state">✅ Значимых отклонений нет</div>
      {% endif %}
    </div>
  </div>

  <!-- Trend Analysis -->
  <div class="section">
    <h2>📈 Анализ тренда</h2>
    <div style="display: flex; align-items: center; gap: 20px; flex-wrap: wrap;">
      <div class="trend-badge" style="background: {{ trend_color }}1a; color: {{ trend_color }};">
        {{ trend_icon }} {{ trend.direction | upper }}
      </div>
      <div>
        <div style="font-size: 1rem;">{{ trend.description }}</div>
        <div style="font-size: 0.85rem; color: #888; margin-top: 4px;">
          Наклон: {{ trend.slope }} · R²: {{ trend.r2 }} · p-value: {{ trend.p_value }}
          {% if trend.is_significant %} · <span style="color: #27ae60;">статистически значимо</span>{% endif %}
        </div>
      </div>
    </div>
    {% if trend.peak_hours %}
    <div style="margin-top: 16px; display: flex; gap: 16px; flex-wrap: wrap;">
      <div>
        <span style="font-size: 0.8rem; color: #888; text-transform: uppercase;">Пиковые часы</span><br>
        {% for h in trend.peak_hours %}<span class="badge badge-red">{{ "%02d:00"|format(h) }}</span> {% endfor %}
      </div>
      <div>
        <span style="font-size: 0.8rem; color: #888; text-transform: uppercase;">Тихие часы</span><br>
        {% for h in trend.quiet_hours %}<span class="badge badge-blue">{{ "%02d:00"|format(h) }}</span> {% endfor %}
      </div>
    </div>
    {% endif %}
  </div>

  <!-- Charts grid -->
  <div class="charts-grid">
    {% if charts.event_pie %}
    <div class="chart-card">{{ charts.event_pie }}</div>
    {% endif %}
    {% if charts.hourly %}
    <div class="chart-card">{{ charts.hourly }}</div>
    {% endif %}
    {% if charts.top_pages %}
    <div class="chart-card chart-full">{{ charts.top_pages }}</div>
    {% endif %}
    {% if charts.session_hist %}
    <div class="chart-card">{{ charts.session_hist }}</div>
    {% endif %}
  </div>

  <!-- Session Stats -->
  <div class="two-col">
    <div class="section">
      <h2>🔁 Статистика сессий</h2>
      <table class="stat-table">
        <tr><th>Метрика</th><th>Значение</th></tr>
        <tr><td>Всего сессий</td><td><strong>{{ sess.total_sessions }}</strong></td></tr>
        <tr><td>Среднее событий/сессия</td><td>{{ sess.avg_events_per_session }}</td></tr>
        <tr><td>Медиана событий/сессия</td><td>{{ sess.median_events_per_session }}</td></tr>
        <tr><td>Средняя длина сессии</td><td>{{ sess.avg_duration_s }}с</td></tr>
        <tr><td>Медиана длины сессии</td><td>{{ sess.median_duration_s }}с</td></tr>
        <tr><td>Максимум событий в сессии</td><td>{{ sess.max_events_session }}</td></tr>
        <tr><td>Сессий-выбросов</td>
          <td><span class="badge {{ 'badge-red' if sess.outlier_sessions_count > 0 else 'badge-green' }}">
            {{ sess.outlier_sessions_count }}
          </span></td>
        </tr>
      </table>
    </div>

    <!-- Anomalous Users -->
    <div class="section">
      <h2>👤 Аномальные пользователи</h2>
      {% set ua = user_anom.anomalous_users %}
      {% if ua %}
      <table class="stat-table">
        <tr><th>User ID</th><th>Событий</th><th>Сессий</th><th>Score</th></tr>
        {% for u in ua[:8] %}
        <tr>
          <td><code>{{ u.user_id[:20] }}</code></td>
          <td>{{ u.event_count }}</td>
          <td>{{ u.session_count }}</td>
          <td><span class="badge badge-orange">{{ u.anomaly_score }}</span></td>
        </tr>
        {% endfor %}
      </table>
      <div style="margin-top: 10px; font-size: 0.85rem; color: #888;">
        Аномальных: {{ user_anom.anomalous_count }} из {{ user_anom.total_users }}
      </div>
      {% else %}
      <div class="empty-state">✅ {{ user_anom.get('note', 'Аномальных пользователей нет') }}</div>
      {% endif %}
    </div>
  </div>

</div>

<div class="footer">
  Clickstream Analyzer · Сгенерировано {{ generated_at }}
</div>
</body>
</html>
"""
