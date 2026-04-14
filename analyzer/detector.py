import pandas as pd
import numpy as np

try:
    from scipy import stats as _scipy_stats
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _linregress(x, y):
    """Minimal linear regression: returns slope, intercept, r2, p_value."""
    if _HAS_SCIPY:
        res = _scipy_stats.linregress(x, y)
        return res.slope, res.intercept, res.rvalue ** 2, res.pvalue
    n = len(x)
    x = np.array(x, dtype=float)
    y = np.array(y, dtype=float)
    x_mean, y_mean = x.mean(), y.mean()
    ss_xy = ((x - x_mean) * (y - y_mean)).sum()
    ss_xx = ((x - x_mean) ** 2).sum()
    if ss_xx == 0:
        return 0.0, y_mean, 0.0, 1.0
    slope = ss_xy / ss_xx
    intercept = y_mean - slope * x_mean
    y_pred = slope * x + intercept
    ss_res = ((y - y_pred) ** 2).sum()
    ss_tot = ((y - y_mean) ** 2).sum()
    r2 = 1 - ss_res / ss_tot if ss_tot != 0 else 0.0
    se = np.sqrt(ss_res / max(n - 2, 1) / ss_xx) if ss_xx > 0 else 0
    t_stat = slope / se if se > 0 else 0
    p_value = 2 * (1 - min(abs(t_stat) / (abs(t_stat) + n), 1))
    return slope, intercept, r2, p_value


def _trend_description(direction, slope, r2, p_value, pct_change) -> str:
    sig = "значимо" if p_value < 0.05 else "незначимо"
    arrows = {"growing": "↑", "declining": "↓", "stable": "→"}
    arrow = arrows.get(direction, "")
    if direction == "stable":
        return f"{arrow} Стабильно (R²={r2:.2f}, {sig})"
    sign = "+" if pct_change > 0 else ""
    return f"{arrow} {sign}{pct_change:.1f}% за период (slope={slope:+.1f}, R²={r2:.2f}, {sig})"


# ─── Группировочные колонки ──────────────────────────────────────────────────

_GROUP_COLS = [
    "metric_id", "metric_name", "channel_name",
    "lvl_1", "segment_name", "lvl_2", "lvl_3", "block_type",
]


# ─── Main entry point ────────────────────────────────────────────────────────

def detect_all(df: pd.DataFrame) -> dict:
    """Run all detection algorithms and return results dict."""
    results = {}
    results["anomalies"] = detect_value_anomalies(df)
    results["trends"] = detect_trends(df)
    results["dod"] = detect_dod(df)
    results["wow"] = detect_wow(df)
    results["top_services"] = detect_top_services(df)
    results["channel_breakdown"] = detect_channel_breakdown(df)
    results["status_screen"] = detect_status_screen(df)
    results["product_dynamics"] = detect_product_dynamics(df)
    # backward-compat aliases
    results["deviations"] = results["dod"]
    return results


# ─── 1. Value anomalies: IQR×3 per (metric_id, lvl_1, lvl_2) ────────────────

def detect_value_anomalies(df: pd.DataFrame) -> list:
    """Flag rows where val is a statistical outlier within its group (IQR×3)."""
    result = []
    for (metric_id, lvl1, lvl2), grp in df.groupby(["metric_id", "lvl_1", "lvl_2"]):
        q1 = grp["val"].quantile(0.25)
        q3 = grp["val"].quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            continue
        upper = q3 + 3 * iqr
        outliers = grp[grp["val"] > upper].copy()
        mean_val = grp["val"].mean()
        std_val = grp["val"].std()

        for _, row in outliers.iterrows():
            z = (row["val"] - mean_val) / std_val if std_val > 0 else 0
            metric_name = str(row.get("metric_name", row["metric_id"]))
            result.append({
                "timestamp": str(row["report_dt"].date()),
                "metric_id": str(row["metric_id"]),
                "metric_name": metric_name,
                "channel_name": str(row.get("channel_name", row.get("event_category_name", ""))),
                "segment_name": str(row.get("segment_name", row["lvl_1"])),
                "lvl_1": str(row["lvl_1"]),
                "lvl_2": str(row["lvl_2"]),
                "lvl_3": str(row["lvl_3"]),
                "block_type": str(row.get("block_type", "")),
                "val": int(row["val"]),
                "threshold": round(float(upper), 0),
                "zscore": round(float(z), 2) if not np.isnan(z) else 0,
                "severity": "high" if row["val"] > upper * 2 else "medium",
                "description": (
                    f"{metric_name} / {row['lvl_1']} / {row['lvl_2']}: "
                    f"val={int(row['val'])} (порог {upper:.0f})"
                ),
            })

    result.sort(key=lambda x: x["val"], reverse=True)
    return result


# ─── 2. Trends: linear regression per metric_name ────────────────────────────

def detect_trends(df: pd.DataFrame) -> dict:
    """Compute daily totals per metric_name and fit a linear trend."""
    df2 = df.copy()
    df2["date"] = df2["report_dt"].dt.date
    daily = df2.groupby(["date", "metric_name"])["val"].sum().reset_index()

    trends = {}
    timeseries = {}

    for metric, grp in daily.groupby("metric_name"):
        grp = grp.sort_values("date")
        y = grp["val"].values.astype(float)
        x = np.arange(len(y))

        if len(y) < 2:
            trends[metric] = {"direction": "insufficient_data", "slope": 0, "r2": 0,
                              "description": "Недостаточно данных"}
            ts_records = grp[["date", "val"]].copy()
            ts_records["date"] = ts_records["date"].astype(str)
            timeseries[metric] = ts_records.to_dict("records")
            continue

        slope, intercept, r2, p_value = _linregress(x, y)

        rel_slope = slope / y.mean() if y.mean() != 0 else 0
        if abs(rel_slope) < 0.02 or r2 < 0.1:
            direction = "stable"
        elif slope > 0:
            direction = "growing"
        else:
            direction = "declining"

        pct_total = round((y[-1] - y[0]) / y[0] * 100, 1) if y[0] != 0 else 0

        trends[metric] = {
            "direction": direction,
            "slope": round(float(slope), 2),
            "r2": round(float(r2), 4),
            "p_value": round(float(p_value), 4),
            "is_significant": bool(p_value < 0.05),
            "pct_change_total": pct_total,
            "description": _trend_description(direction, slope, r2, p_value, pct_total),
        }
        ts_records = grp[["date", "val"]].copy()
        ts_records["date"] = ts_records["date"].astype(str)
        timeseries[metric] = ts_records.to_dict("records")

    combined = df2.groupby("date")["val"].sum().reset_index()
    combined["date"] = combined["date"].astype(str)

    return {
        "per_metric": trends,
        "timeseries": timeseries,
        "combined_timeseries": combined.to_dict("records"),
    }


# ─── 3. DoD: each weekday vs same weekday -7 days (no Sundays) ───────────────

def detect_dod(df: pd.DataFrame) -> list:
    """Compare each weekday vs same weekday -7 days across all available dates.
    Excludes Sundays (weekday == 6).
    Threshold: |pct| > 50 and |delta| > 5.
    Returns all deviations sorted by |delta| desc (reporter limits to top 50).
    """
    import datetime as _dt

    WEEKDAY_RU = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]

    df2 = df.copy()
    df2["date"] = df2["report_dt"].dt.date
    dates_set = set(df2["date"].unique())
    dates = sorted(dates_set)

    if len(dates) < 2:
        return []

    available_cols = [c for c in _GROUP_COLS if c in df2.columns]
    grp_cols = available_cols

    # Take only last 7 non-Sunday dates
    non_sunday_dates = [d for d in dates if d.weekday() != 6]
    recent_dates = non_sunday_dates[-7:] if len(non_sunday_dates) >= 7 else non_sunday_dates

    all_deviations = []

    for d_curr in recent_dates:
        d_prev = d_curr - _dt.timedelta(days=7)
        if d_prev not in dates_set:
            continue

        curr_df = df2[df2["date"] == d_curr].groupby(grp_cols)["val"].sum().reset_index().rename(columns={"val": "val_curr"})
        prev_df = df2[df2["date"] == d_prev].groupby(grp_cols)["val"].sum().reset_index().rename(columns={"val": "val_prev"})

        merged = curr_df.merge(prev_df, on=grp_cols, how="outer").fillna(0)
        merged["val_curr"] = merged["val_curr"].astype(int)
        merged["val_prev"] = merged["val_prev"].astype(int)
        merged = merged[merged["val_prev"] > 0].copy()
        merged["delta"] = merged["val_curr"] - merged["val_prev"]
        merged["pct"] = (merged["delta"] / merged["val_prev"] * 100).round(1)

        flagged = merged[(merged["pct"].abs() > 50) & (merged["delta"].abs() > 5)].copy()

        weekday_name = WEEKDAY_RU[d_curr.weekday()]

        for _, row in flagged.iterrows():
            r = {
                "date_from": str(d_prev),
                "date_to": str(d_curr),
                "weekday": weekday_name,
                "direction": "spike" if row["delta"] > 0 else "drop",
                "delta": int(row["delta"]),
                "pct": float(row["pct"]),
                "val_prev": int(row["val_prev"]),
                "val_curr": int(row["val_curr"]),
            }
            for col in grp_cols:
                r[col] = str(row[col]) if col in row else ""
            all_deviations.append(r)

    all_deviations.sort(key=lambda x: abs(x["delta"]), reverse=True)
    return all_deviations


# ─── 4. WoW: last 7 days vs previous 7 days (full week vs full week) ──────────

def detect_wow(df: pd.DataFrame) -> list:
    """Compare last full 7 days vs previous full 7 days.
    Threshold: |pct| > 30 and |delta| > 5, sorted by |delta| desc.
    """
    import datetime as _dt

    df2 = df.copy()
    df2["date"] = df2["report_dt"].dt.date
    dates = sorted(df2["date"].unique())

    if not dates:
        return []

    d_curr_end = dates[-1]
    d_curr_start = d_curr_end - _dt.timedelta(days=6)   # текущая неделя: 7 дней
    d_prev_end   = d_curr_end - _dt.timedelta(days=7)   # предыдущая неделя: 7 дней
    d_prev_start = d_curr_end - _dt.timedelta(days=13)

    curr_week = df2[(df2["date"] >= d_curr_start) & (df2["date"] <= d_curr_end)]
    prev_week = df2[(df2["date"] >= d_prev_start) & (df2["date"] <= d_prev_end)]

    if curr_week.empty or prev_week.empty:
        return []

    available_cols = [c for c in _GROUP_COLS if c in df2.columns]
    grp_cols = available_cols

    curr = curr_week.groupby(grp_cols)["val"].sum().reset_index().rename(columns={"val": "val_curr"})
    prev = prev_week.groupby(grp_cols)["val"].sum().reset_index().rename(columns={"val": "val_prev"})

    merged = curr.merge(prev, on=grp_cols, how="outer").fillna(0)
    merged["val_curr"] = merged["val_curr"].astype(int)
    merged["val_prev"] = merged["val_prev"].astype(int)

    merged = merged[merged["val_prev"] > 0].copy()
    merged["delta"] = merged["val_curr"] - merged["val_prev"]
    merged["pct"] = (merged["delta"] / merged["val_prev"] * 100).round(1)

    flagged = merged[(merged["pct"].abs() > 30) & (merged["delta"].abs() > 5)].copy()
    flagged = flagged.sort_values("delta", key=abs, ascending=False)

    result = []
    for _, row in flagged.iterrows():
        r = {
            "date_from": str(d_prev_start),
            "date_to": str(d_curr_end),
            "curr_week_start": str(d_curr_start),
            "curr_week_end": str(d_curr_end),
            "prev_week_start": str(d_prev_start),
            "prev_week_end": str(d_prev_end),
            "direction": "spike" if row["delta"] > 0 else "drop",
            "delta": int(row["delta"]),
            "pct": float(row["pct"]),
            "val_prev": int(row["val_prev"]),
            "val_curr": int(row["val_curr"]),
        }
        for col in grp_cols:
            r[col] = str(row[col]) if col in row else ""
        result.append(r)

    return result


# ─── 5. Top services ─────────────────────────────────────────────────────────

def detect_top_services(df: pd.DataFrame) -> list:
    """Top 30 log_name values by total val."""
    if "log_name" not in df.columns:
        return []
    top = (
        df.groupby("log_name")["val"]
        .sum()
        .sort_values(ascending=False)
        .head(30)
        .reset_index()
    )
    return [{"log_name": str(r["log_name"]), "total_val": int(r["val"])} for _, r in top.iterrows()]


# ─── 6. Channel breakdown ─────────────────────────────────────────────────────

def detect_channel_breakdown(df: pd.DataFrame) -> dict:
    """Val by day for each channel_name, plus segment breakdown."""
    df2 = df.copy()
    df2["date"] = df2["report_dt"].dt.date.astype(str)

    # by channel per day
    ch_day = (
        df2.groupby(["date", "channel_name"])["val"]
        .sum()
        .reset_index()
    )
    by_channel = {}
    for ch, grp in ch_day.groupby("channel_name"):
        by_channel[ch] = grp.set_index("date")["val"].to_dict()

    # by segment total
    by_segment = (
        df2.groupby("segment_name")["val"]
        .sum()
        .sort_values(ascending=False)
        .to_dict()
    )
    by_segment = {k: int(v) for k, v in by_segment.items()}

    # by channel total
    by_channel_total = (
        df2.groupby("channel_name")["val"]
        .sum()
        .sort_values(ascending=False)
        .to_dict()
    )
    by_channel_total = {k: int(v) for k, v in by_channel_total.items()}

    # by block_type per day (for timeline chart)
    bt_day = (
        df2.groupby(["date", "block_type"])["val"]
        .sum()
        .reset_index()
    )
    by_block = {}
    for bt, grp in bt_day.groupby("block_type"):
        by_block[bt] = {d: int(v) for d, v in grp.set_index("date")["val"].items()}

    # all dates sorted
    all_dates = sorted(df2["date"].unique())

    return {
        "by_channel_per_day": {k: {d: int(v) for d, v in vv.items()} for k, vv in by_channel.items()},
        "by_channel_total": by_channel_total,
        "by_segment_total": by_segment,
        "by_block_per_day": by_block,
        "dates": all_dates,
    }


# ─── 7. Status screen (55558) breakdown ──────────────────────────────────────

def detect_status_screen(df: pd.DataFrame) -> dict:
    """For metric_id=55558: breakdown by lvl_4 per day and total."""
    ss = df[df["metric_id_int"] == 55558].copy() if "metric_id_int" in df.columns else \
         df[df["metric_id"].astype(str) == "55558"].copy()

    if ss.empty:
        return {"total": {}, "by_day": {}, "total_val": 0}

    ss["date"] = ss["report_dt"].dt.date.astype(str)

    # total by lvl_4
    total = (
        ss.groupby("lvl_4")["val"]
        .sum()
        .sort_values(ascending=False)
        .to_dict()
    )
    total = {k: int(v) for k, v in total.items()}

    # by day and lvl_4
    day_lvl4 = (
        ss.groupby(["date", "lvl_4"])["val"]
        .sum()
        .reset_index()
    )
    by_day = {}
    for date, grp in day_lvl4.groupby("date"):
        by_day[date] = {str(r["lvl_4"]): int(r["val"]) for _, r in grp.iterrows()}

    return {
        "total": total,
        "by_day": by_day,
        "total_val": int(ss["val"].sum()),
    }


# ─── Product dynamics (heatmap data) ─────────────────────────────────────────

def detect_product_dynamics(df: pd.DataFrame, top_n: int = 40) -> dict:
    """
    Build per-product (lvl_2) daily val matrix for heatmap.
    Returns top_n products by total val, with:
      - dates: sorted list of date strings
      - products: list of {name, total_val, metric_name, segment}
      - matrix: dict {product_name: {date: val}}
      - dod_pct: dict {product_name: pct_change D vs D-1 for last date}
      - wow_pct: dict {product_name: pct_change D vs D-7 for last date}
    """
    import datetime as _dt

    df2 = df.copy()
    df2["date"] = df2["report_dt"].dt.date.astype(str)

    # Top products by total val
    top_products = (
        df2.groupby(["lvl_2", "metric_name", "segment_name"])["val"]
        .sum()
        .reset_index()
        .sort_values("val", ascending=False)
    )
    top_products = top_products.drop_duplicates("lvl_2").head(top_n)
    product_names = top_products["lvl_2"].tolist()

    # All dates
    dates = sorted(df2["date"].unique())
    last_date = dates[-1] if dates else None

    # ── Summary matrix (product × date) ──────────────────────────────────────
    daily_sum = (
        df2[df2["lvl_2"].isin(product_names)]
        .groupby(["lvl_2", "date"])["val"]
        .sum()
        .reset_index()
    )
    matrix = {p: {} for p in product_names}
    for _, row in daily_sum.iterrows():
        matrix[row["lvl_2"]][row["date"]] = int(row["val"])

    # ── Drill-down: product × block_type × date ───────────────────────────────
    daily_block = (
        df2[df2["lvl_2"].isin(product_names)]
        .groupby(["lvl_2", "block_type", "date"])["val"]
        .sum()
        .reset_index()
    )
    # {product: {block_type: {date: val}}}
    block_matrix = {p: {} for p in product_names}
    for _, row in daily_block.iterrows():
        p, bt, d, v = row["lvl_2"], row["block_type"], row["date"], int(row["val"])
        if bt not in block_matrix[p]:
            block_matrix[p][bt] = {}
        block_matrix[p][bt][d] = v

    # ── Drill-down: product × lvl_4 × date (для Статусного экрана 55558) ──────
    lvl4_matrix = {}
    if "lvl_4" in df2.columns and df2["lvl_4"].notna().any():
        daily_lvl4 = (
            df2[df2["lvl_2"].isin(product_names) & df2["lvl_4"].notna()]
            .groupby(["lvl_2", "lvl_4", "date"])["val"]
            .sum()
            .reset_index()
        )
        lvl4_matrix = {p: {} for p in product_names}
        for _, row in daily_lvl4.iterrows():
            p, lv, d, v = row["lvl_2"], str(row["lvl_4"]), row["date"], int(row["val"])
            if lv not in lvl4_matrix[p]:
                lvl4_matrix[p][lv] = {}
            lvl4_matrix[p][lv][d] = v

    # ── Drill-down: product × channel × date ─────────────────────────────────
    daily_channel = (
        df2[df2["lvl_2"].isin(product_names)]
        .groupby(["lvl_2", "channel_name", "date"])["val"]
        .sum()
        .reset_index()
    )
    channel_matrix = {p: {} for p in product_names}
    for _, row in daily_channel.iterrows():
        p, ch, d, v = row["lvl_2"], row["channel_name"], row["date"], int(row["val"])
        if ch not in channel_matrix[p]:
            channel_matrix[p][ch] = {}
        channel_matrix[p][ch][d] = v

    # ── DoD / WoW per product ─────────────────────────────────────────────────
    dod_pct = {}
    wow_pct = {}
    dod_delta = {}
    wow_delta = {}

    if last_date and len(dates) >= 2:
        prev_date = dates[-2]
        for p in product_names:
            curr = matrix[p].get(last_date, 0)
            prev = matrix[p].get(prev_date, 0)
            if prev > 0:
                dod_pct[p] = round((curr - prev) / prev * 100, 1)
                dod_delta[p] = curr - prev

    if last_date:
        d_end = _dt.date.fromisoformat(last_date)
        curr_week_dates = [str(d_end - _dt.timedelta(days=i)) for i in range(7)]
        prev_week_dates = [str(d_end - _dt.timedelta(days=7 + i)) for i in range(7)]
        for p in product_names:
            curr_sum = sum(matrix[p].get(d, 0) for d in curr_week_dates)
            prev_sum = sum(matrix[p].get(d, 0) for d in prev_week_dates)
            if prev_sum > 0:
                wow_pct[p] = round((curr_sum - prev_sum) / prev_sum * 100, 1)
                wow_delta[p] = curr_sum - prev_sum

    # ── Per-product trend: current week avg vs previous week avg ─────────────
    dates_str = [str(d) for d in dates]
    product_trends = {}
    for p in product_names:
        vals = [matrix[p].get(d, 0) for d in dates_str]
        n = len(vals)
        if n >= 2:
            # Split into two halves (each up to 7 days), compare averages
            half = min(7, n // 2) if n >= 4 else 1
            curr_avg = np.mean(vals[-half:]) if half > 0 else 0
            prev_avg = np.mean(vals[:half])  if half > 0 else 0
            if prev_avg > 0:
                pct_chg = round((curr_avg - prev_avg) / prev_avg * 100, 1)
            else:
                pct_chg = 0.0
            if abs(pct_chg) < 10:
                direction = "stable"
            elif pct_chg > 0:
                direction = "growing"
            else:
                direction = "declining"
        else:
            direction = "stable"
            pct_chg = 0.0
        product_trends[p] = {"direction": direction, "pct_change": pct_chg}

    products_meta = []
    for _, row in top_products.iterrows():
        p = str(row["lvl_2"])
        tr = product_trends.get(p, {})
        products_meta.append({
            "name": p,
            "total_val": int(row["val"]),
            "metric_name": str(row["metric_name"]),
            "segment": str(row["segment_name"]),
            "dod_pct": dod_pct.get(p),
            "dod_delta": dod_delta.get(p),
            "wow_pct": wow_pct.get(p),
            "wow_delta": wow_delta.get(p),
            "last_val": matrix[p].get(last_date, 0) if last_date else 0,
            "trend_direction": tr.get("direction", "stable"),
            "trend_pct": tr.get("pct_change", 0.0),
        })

    return {
        "dates": dates,
        "products": products_meta,
        "matrix": matrix,
        "block_matrix": block_matrix,
        "lvl4_matrix": lvl4_matrix,
        "channel_matrix": channel_matrix,
        "dod_pct": dod_pct,
        "wow_pct": wow_pct,
        "dod_delta": dod_delta,
        "wow_delta": wow_delta,
        "last_date": last_date,
    }
