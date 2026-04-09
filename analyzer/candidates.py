"""
Кандидаты в Проблемы — сравнение последних 7 дней с предыдущими 7 днями.
Порог: рост > 50% считается кандидатом.
"""
import pandas as pd
import datetime as _dt
from pathlib import Path
from jinja2 import Template


THRESHOLD_PCT = 50  # % роста → кандидат


# ─── Detection ────────────────────────────────────────────────────────────────

def _get_periods(df: pd.DataFrame):
    last_date  = df["report_dt"].dt.date.max()
    curr_end   = last_date
    curr_start = last_date - _dt.timedelta(days=6)
    prev_end   = last_date - _dt.timedelta(days=7)
    prev_start = last_date - _dt.timedelta(days=13)
    return curr_start, curr_end, prev_start, prev_end


def detect_tech_error_candidates(df: pd.DataFrame, metric_id: str) -> dict:
    """
    Compare last 7 days vs previous 7 days for given metric_id.
    Group by (lvl_2, lvl_3, block_type).
    Returns candidates (growth > THRESHOLD_PCT) and full comparison table.
    """
    df_m = df[df["metric_id"] == metric_id].copy()
    if df_m.empty:
        return {"candidates": [], "all_rows": [], "periods": {}, "metric_id": metric_id}

    df_m["date"] = df_m["report_dt"].dt.date
    curr_start, curr_end, prev_start, prev_end = _get_periods(df_m)

    df_curr = df_m[(df_m["date"] >= curr_start) & (df_m["date"] <= curr_end)]
    df_prev = df_m[(df_m["date"] >= prev_start) & (df_m["date"] <= prev_end)]

    group_cols = ["lvl_2", "lvl_3", "block_type", "segment_name", "channel_name"]

    agg_curr = df_curr.groupby(group_cols)["val"].sum().reset_index().rename(columns={"val": "curr_val"})
    agg_prev = df_prev.groupby(group_cols)["val"].sum().reset_index().rename(columns={"val": "prev_val"})

    merged = pd.merge(agg_curr, agg_prev, on=group_cols, how="outer").fillna(0)
    merged["curr_val"] = merged["curr_val"].astype(int)
    merged["prev_val"] = merged["prev_val"].astype(int)
    merged["delta"]    = merged["curr_val"] - merged["prev_val"]
    merged["pct"]      = merged.apply(
        lambda r: round((r["delta"] / r["prev_val"]) * 100, 1) if r["prev_val"] > 0 else None,
        axis=1,
    )
    merged["is_candidate"] = merged["pct"].apply(lambda p: p is not None and p > THRESHOLD_PCT)
    merged = merged.sort_values(["is_candidate", "curr_val"], ascending=[False, False])

    def to_row(r):
        return {
            "lvl_2":        str(r["lvl_2"]),
            "lvl_3":        str(r["lvl_3"]),
            "block_type":   str(r["block_type"]),
            "segment":      str(r["segment_name"]),
            "channel":      str(r["channel_name"]),
            "curr_val":     int(r["curr_val"]),
            "prev_val":     int(r["prev_val"]),
            "delta":        int(r["delta"]),
            "pct":          r["pct"],
            "is_candidate": bool(r["is_candidate"]),
        }

    all_rows   = [to_row(r) for _, r in merged.iterrows()]
    candidates = [row for row in all_rows if row["is_candidate"]]

    return {
        "metric_id":     metric_id,
        "candidates":    candidates,
        "all_rows":      all_rows,
        "periods":       {
            "curr_start": str(curr_start),
            "curr_end":   str(curr_end),
            "prev_start": str(prev_start),
            "prev_end":   str(prev_end),
        },
        "threshold_pct": THRESHOLD_PCT,
    }


def build_candidates_results(df: pd.DataFrame) -> dict:
    return {
        "tech_error": detect_tech_error_candidates(df, "55556"),
        # разделы 2 и 3 — будут добавлены позже
    }


# ─── Report ───────────────────────────────────────────────────────────────────

def generate_candidates_report(df: pd.DataFrame, output_path: str) -> str:
    results = build_candidates_results(df)
    html = _render(results)
    Path(output_path).write_text(html, encoding="utf-8")
    return output_path


def _render(results: dict) -> str:
    import datetime
    generated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    template = Template(CANDIDATES_TEMPLATE)
    return template.render(generated_at=generated_at, results=results)


# ─── HTML Template ─────────────────────────────────────────────────────────────
# NOTE: macro must be declared before first use in Jinja2

CANDIDATES_TEMPLATE = r"""
{% macro render_table(rows, tbl_id) %}
{% if not rows %}
<div class="empty-state">Нет данных</div>
{% else %}
{% set max_val = namespace(v=1) %}
{% for r in rows %}{% if r.curr_val > max_val.v %}{% set max_val.v = r.curr_val %}{% endif %}{% endfor %}
<div style="overflow-x:auto;">
<table id="{{ tbl_id }}_tbl">
  <thead>
    <tr>
      <th></th>
      <th>Продукт</th>
      <th>Блок</th>
      <th>Тип блока</th>
      <th>Сегмент</th>
      <th>Канал</th>
      <th style="text-align:right">Пред. 7д</th>
      <th style="text-align:right">Тек. 7д</th>
      <th style="text-align:right">Δval</th>
      <th style="text-align:right">Δ%</th>
      <th style="width:90px"></th>
    </tr>
  </thead>
  <tbody>
  {% for r in rows %}
  {% set is_cand = r.is_candidate %}
  {% set bar_w = ((r.curr_val / max_val.v) * 86) | int %}
  <tr class="{{ 'candidate' if is_cand else '' }}">
    <td>{{ "🚨" if is_cand else "" }}</td>
    <td><strong>{{ r.lvl_2 }}</strong></td>
    <td style="color:#555;">{{ r.lvl_3 }}</td>
    <td><span class="block-pill block-{{ r.block_type }}">{{ r.block_type }}</span></td>
    <td style="color:#666;font-size:0.8rem;">{{ r.segment }}</td>
    <td style="color:#666;font-size:0.8rem;">{{ r.channel }}</td>
    <td style="text-align:right;color:#888;">{{ "{:,}".format(r.prev_val) }}</td>
    <td style="text-align:right;font-weight:600;">{{ "{:,}".format(r.curr_val) }}</td>
    <td style="text-align:right;font-weight:700;
               color:{{ '#e74c3c' if r.delta > 0 else ('#27ae60' if r.delta < 0 else '#888') }};">
      {{ '+' if r.delta > 0 else '' }}{{ "{:,}".format(r.delta) }}
    </td>
    <td style="text-align:right;">
      {% if r.pct is not none %}
        <span class="badge {{ 'badge-red' if r.pct > 50 else ('badge-green' if r.pct < 0 else 'badge-gray') }}">
          {{ '+' if r.pct > 0 else '' }}{{ r.pct }}%
        </span>
      {% else %}
        <span class="badge badge-orange">new</span>
      {% endif %}
    </td>
    <td>
      <div class="inline-bar"
           style="width:{{ bar_w }}px;background:{{ '#e74c3c' if r.is_candidate else ('#3498db' if r.delta < 0 else '#95a5a6') }};"></div>
    </td>
  </tr>
  {% endfor %}
  </tbody>
</table>
</div>
{% endif %}
{% endmacro %}
<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Кандидаты в Проблемы — СБОЛ.про</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
           background: #f0f2f5; color: #1a1a2e; line-height: 1.6; }
    .header { background: linear-gradient(135deg, #1a1a2e 0%, #6d1f1f 60%, #c0392b 100%);
              color: white; padding: 36px 40px; }
    .header h1 { font-size: 1.9rem; font-weight: 700; margin-bottom: 6px; }
    .header .meta { opacity: 0.75; font-size: 0.88rem; }
    .container { max-width: 1400px; margin: 0 auto; padding: 24px; }
    .section { background: white; border-radius: 12px; padding: 24px;
               margin-bottom: 24px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
    .section-header { display: flex; align-items: center; gap: 12px;
                      padding-bottom: 14px; border-bottom: 2px solid #f0f2f5; margin-bottom: 16px; flex-wrap: wrap; }
    .section-header h2 { font-size: 1.1rem; font-weight: 700; }
    .period-badge { font-size: 0.78rem; color: #888; background: #f0f2f5;
                    padding: 3px 10px; border-radius: 20px; margin-left: auto; }
    .summary-row { display: flex; gap: 16px; margin-bottom: 18px; flex-wrap: wrap; }
    .kpi { background: #f8f9fa; border-radius: 8px; padding: 14px 20px; text-align: center; min-width: 120px; }
    .kpi .num { font-size: 1.6rem; font-weight: 700; }
    .kpi .lbl { font-size: 0.73rem; color: #888; text-transform: uppercase; margin-top: 3px; }
    .kpi.red .num   { color: #e74c3c; }
    .kpi.green .num { color: #27ae60; }
    .kpi.blue .num  { color: #3498db; }
    table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
    th { text-align: left; padding: 8px 10px; background: #f8f9fa;
         font-size: 0.74rem; text-transform: uppercase; color: #888; white-space: nowrap;
         position: sticky; top: 0; z-index: 1; }
    td { padding: 7px 10px; border-bottom: 1px solid #f4f4f4; vertical-align: middle; }
    tr:last-child td { border-bottom: none; }
    tr.candidate td  { background: #fff8f8; }
    tr.candidate:hover td { background: #fff0f0; }
    tr:not(.candidate):hover td { background: #fafafa; }
    .badge { display: inline-block; padding: 2px 9px; border-radius: 12px;
             font-size: 0.72rem; font-weight: 600; white-space: nowrap; }
    .badge-red    { background: #fde8e8; color: #c0392b; }
    .badge-green  { background: #e8f8ee; color: #1e8449; }
    .badge-blue   { background: #e8f0fe; color: #2471a3; }
    .badge-gray   { background: #f0f0f0; color: #888; }
    .badge-orange { background: #fff3cd; color: #d68910; }
    .block-pill { display: inline-block; padding: 1px 7px; border-radius: 8px;
                  font-size: 0.72rem; font-weight: 600; }
    .block-боевой    { background: #fde8e8; color: #c0392b; }
    .block-пилотный  { background: #e8f8ee; color: #1e8449; }
    .block-резервный { background: #e8f0fe; color: #2471a3; }
    .block-неизвестный { background: #f0f0f0; color: #888; }
    .inline-bar { height: 8px; border-radius: 3px; display: inline-block; min-width: 2px; }
    .empty-state { text-align: center; padding: 24px; color: #aaa; }
    .footer { text-align: center; padding: 28px; color: #aaa; font-size: 0.83rem; }
    .tab-nav { display: flex; gap: 4px; margin-bottom: 0; border-bottom: 2px solid #e0e0e0; }
    .tab-btn { padding: 8px 18px; border: none; background: none; cursor: pointer;
               font-size: 0.9rem; color: #888; border-bottom: 2px solid transparent;
               margin-bottom: -2px; transition: all 0.15s; border-radius: 4px 4px 0 0; }
    .tab-btn.active { color: #e74c3c; border-bottom-color: #e74c3c; font-weight: 700; background: #fff8f8; }
    .tab-btn:hover:not(.active) { background: #f8f8f8; }
    .tab-content { display: none; padding-top: 16px; }
    .tab-content.active { display: block; }
  </style>
</head>
<body>

<div class="header">
  <h1>🚨 Кандидаты в Проблемы — СБОЛ.про</h1>
  <div class="meta">
    Сравнение последних 7 дней vs предыдущие 7 дней
    &nbsp;·&nbsp; Порог роста: +{{ results.tech_error.threshold_pct }}%
    &nbsp;·&nbsp; Сгенерировано: {{ generated_at }}
  </div>
</div>

<div class="container">

  {# ── Раздел 1: Тех Ошибка (55556) ── #}
  {% set te = results.tech_error %}
  {% set p  = te.periods %}
  <div class="section">
    <div class="section-header">
      <span style="font-size:1.4rem;">⚠️</span>
      <h2>Тех Ошибка &nbsp;<span style="font-weight:400;color:#888;font-size:0.85rem;">metric_id 55556</span></h2>
      <span class="period-badge">
        Текущий: {{ p.curr_start }} → {{ p.curr_end }}
        &nbsp;·&nbsp;
        Предыдущий: {{ p.prev_start }} → {{ p.prev_end }}
      </span>
    </div>

    {% if te.all_rows %}
    <div class="summary-row">
      <div class="kpi red">
        <div class="num">{{ te.candidates | length }}</div>
        <div class="lbl">Кандидатов (&gt;{{ te.threshold_pct }}%)</div>
      </div>
      <div class="kpi blue">
        <div class="num">{{ te.all_rows | length }}</div>
        <div class="lbl">Пар продукт / блок</div>
      </div>
      <div class="kpi green">
        <div class="num">{{ te.all_rows | selectattr('delta', 'lt', 0) | list | length }}</div>
        <div class="lbl">Снижение</div>
      </div>
      <div class="kpi">
        <div class="num">{{ te.all_rows | selectattr('pct', 'none') | list | length }}</div>
        <div class="lbl">Новые (нет истории)</div>
      </div>
    </div>

    <div class="tab-nav">
      <button class="tab-btn active" onclick="switchTab(this,'cand_te','te')">
        🚨 Кандидаты ({{ te.candidates | length }})
      </button>
      <button class="tab-btn" onclick="switchTab(this,'all_te','te')">
        📋 Все пары ({{ te.all_rows | length }})
      </button>
    </div>
    <div id="cand_te" class="tab-content active">
      {% if te.candidates %}
        {{ render_table(te.candidates, 'cand_te') }}
      {% else %}
        <div class="empty-state">✅ Кандидатов нет — рост ниже {{ te.threshold_pct }}%</div>
      {% endif %}
    </div>
    <div id="all_te" class="tab-content">
      {{ render_table(te.all_rows, 'all_te') }}
    </div>
    {% else %}
    <div class="empty-state">Нет данных по metric_id 55556</div>
    {% endif %}
  </div>

  {# Разделы 2 и 3 будут добавлены позже #}

</div>

<div class="footer">Кандидаты в Проблемы · СБОЛ.про · {{ generated_at }}</div>

<script>
function switchTab(btn, tabId, group) {
  document.querySelectorAll('.tab-content').forEach(el => {
    if (el.id && el.id.endsWith('_' + group)) el.classList.remove('active');
  });
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById(tabId).classList.add('active');
  btn.classList.add('active');
}
</script>

</body>
</html>
"""
