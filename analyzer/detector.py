import pandas as pd
import numpy as np

try:
    from scipy import stats as _scipy_stats
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False


def _linregress(x, y):
    """Minimal linear regression: returns slope, intercept, r2, p_value."""
    if _HAS_SCIPY:
        res = _scipy_stats.linregress(x, y)
        return res.slope, res.intercept, res.rvalue ** 2, res.pvalue
    # Pure numpy fallback
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
    # Approximate p-value via t-distribution (2-tailed)
    se = np.sqrt(ss_res / max(n - 2, 1) / ss_xx) if ss_xx > 0 else 0
    t_stat = slope / se if se > 0 else 0
    # rough p-value: small t → large p
    p_value = 2 * (1 - min(abs(t_stat) / (abs(t_stat) + n), 1))
    return slope, intercept, r2, p_value


def detect_all(df: pd.DataFrame) -> dict:
    """Run all detection algorithms and return results."""
    results = {}
    results["anomalies"] = detect_value_anomalies(df)
    results["trends"] = detect_trends(df)
    results["deviations"] = detect_day_over_day(df)
    results["workflow_anomalies"] = detect_workflow_anomalies(df)
    results["environment_stats"] = environment_analysis(df)
    results["metric_stats"] = metric_analysis(df)
    results["workflow_stats"] = workflow_analysis(df)
    return results


# ---------------------------------------------------------------------------
# 1. Outlier detection on raw val (IQR × 3 fence, applied per metric)
# ---------------------------------------------------------------------------

def detect_value_anomalies(df: pd.DataFrame) -> list:
    """
    Flag rows where val is a statistical outlier within its metric group.
    Uses the 3×IQR fence (same threshold validated in the analysis session).
    """
    result = []
    for metric_id, grp in df.groupby("metric_id"):
        q1 = grp["val"].quantile(0.25)
        q3 = grp["val"].quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            continue
        upper = q3 + 3 * iqr
        outliers = grp[grp["val"] > upper].copy()

        for _, row in outliers.iterrows():
            z = (row["val"] - grp["val"].mean()) / grp["val"].std()
            result.append({
                "timestamp": str(row["report_dt"].date()),
                "metric_id": str(row["metric_id"]),
                "metric_name": str(row.get("metric_name", row["metric_id"])),
                "platform": str(row["event_category_name"]),
                "category": str(row["lvl_1"]),
                "workflow": str(row["lvl_2"]),
                "environment": str(row["lvl_3"]),
                "val": int(row["val"]),
                "threshold": round(float(upper), 0),
                "zscore": round(float(z), 2) if not np.isnan(z) else 0,
                "severity": "high" if row["val"] > upper * 2 else "medium",
                "description": (
                    f"{row.get('metric_name', row['metric_id'])} / "
                    f"{row['lvl_1']} / {row['lvl_2']} / {row['lvl_3']}: "
                    f"val={int(row['val'])} (порог {upper:.0f}, z={z:.1f})"
                ),
            })

    # Sort by val descending
    result.sort(key=lambda x: x["val"], reverse=True)
    return result


# ---------------------------------------------------------------------------
# 2. Day-over-day trend per metric
# ---------------------------------------------------------------------------

def detect_trends(df: pd.DataFrame) -> dict:
    """
    Compute daily totals per metric_name and fit a linear trend.
    Returns per-metric trend summaries + a combined timeseries for charting.
    """
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
            timeseries[metric] = grp[["date", "val"]].to_dict("records")
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

    # Combined daily total across all metrics
    combined = df2.groupby("date")["val"].sum().reset_index()
    combined["date"] = combined["date"].astype(str)

    return {
        "per_metric": trends,
        "timeseries": timeseries,
        "combined_timeseries": combined.to_dict("records"),
    }


# ---------------------------------------------------------------------------
# 3. Day-over-day deviations per (metric, lvl_1, lvl_2, lvl_3)
# ---------------------------------------------------------------------------

def detect_day_over_day(df: pd.DataFrame) -> list:
    """
    For each combination (metric_id, lvl_1, lvl_2, lvl_3) compare consecutive days.
    Flags changes > ±80 % or absolute jumps that are outliers.
    """
    df2 = df.copy()
    df2["date"] = df2["report_dt"].dt.date

    key_cols = ["metric_id", "metric_name", "event_category_name", "lvl_1", "lvl_2", "lvl_3"]
    pivot = (
        df2.groupby(key_cols + ["date"])["val"]
        .sum()
        .reset_index()
        .pivot_table(index=key_cols, columns="date", values="val")
        .reset_index()
    )
    pivot.columns.name = None
    dates = sorted(df2["date"].unique())

    deviations = []
    for i in range(1, len(dates)):
        d_prev, d_curr = dates[i - 1], dates[i]
        if d_prev not in pivot.columns or d_curr not in pivot.columns:
            continue

        sub = pivot[[*key_cols, d_prev, d_curr]].dropna(subset=[d_prev, d_curr])
        sub = sub[sub[d_prev] > 0].copy()
        sub["pct"] = (sub[d_curr] - sub[d_prev]) / sub[d_prev] * 100
        sub["delta"] = sub[d_curr] - sub[d_prev]

        # Flag > ±80 % change with non-trivial absolute delta
        flagged = sub[sub["pct"].abs() > 80].copy()
        # Also require delta to be meaningful (> 10)
        flagged = flagged[flagged["delta"].abs() > 10]

        for _, row in flagged.iterrows():
            deviations.append({
                "date_from": str(d_prev),
                "date_to": str(d_curr),
                "metric_name": str(row["metric_name"]),
                "platform": str(row["event_category_name"]),
                "category": str(row["lvl_1"]),
                "workflow": str(row["lvl_2"]),
                "environment": str(row["lvl_3"]),
                "val_prev": int(row[d_prev]),
                "val_curr": int(row[d_curr]),
                "delta": int(row["delta"]),
                "pct": round(float(row["pct"]), 1),
                "direction": "spike" if row["pct"] > 0 else "drop",
            })

    deviations.sort(key=lambda x: abs(x["pct"]), reverse=True)
    return deviations


# ---------------------------------------------------------------------------
# 4. Workflow-level anomaly detection (Isolation Forest style via Z-score)
# ---------------------------------------------------------------------------

def detect_workflow_anomalies(df: pd.DataFrame) -> dict:
    """
    For each (metric_id, lvl_1, lvl_2) compute total val across all dates/envs.
    Flag workflows whose total val is a Z-score outlier (|z| > 2.5).
    """
    df2 = df.copy()
    wf_stats = (
        df2.groupby(["metric_id", "metric_name", "lvl_1", "lvl_2"])
        .agg(
            total_val=("val", "sum"),
            mean_val=("val", "mean"),
            max_val=("val", "max"),
            n_records=("val", "count"),
            n_envs=("lvl_3", "nunique"),
            n_dates=("report_dt", "nunique"),
        )
        .reset_index()
    )

    anomalous = []
    for metric, grp in wf_stats.groupby("metric_id"):
        if len(grp) < 3:
            continue
        mean = grp["total_val"].mean()
        std = grp["total_val"].std()
        if std == 0:
            continue
        grp = grp.copy()
        grp["zscore"] = (grp["total_val"] - mean) / std
        flagged = grp[grp["zscore"].abs() > 2.5]
        for _, row in flagged.iterrows():
            anomalous.append({
                "metric_name": str(row["metric_name"]),
                "category": str(row["lvl_1"]),
                "workflow": str(row["lvl_2"]),
                "total_val": int(row["total_val"]),
                "mean_val": round(float(row["mean_val"]), 1),
                "max_val": int(row["max_val"]),
                "n_envs": int(row["n_envs"]),
                "n_dates": int(row["n_dates"]),
                "zscore": round(float(row["zscore"]), 2),
            })

    anomalous.sort(key=lambda x: abs(x["zscore"]), reverse=True)

    return {
        "anomalous_workflows": anomalous,
        "total_workflows": int(len(wf_stats)),
        "anomalous_count": len(anomalous),
    }


# ---------------------------------------------------------------------------
# 5. Environment analysis
# ---------------------------------------------------------------------------

def environment_analysis(df: pd.DataFrame) -> dict:
    """Top environments by total val, with per-date breakdown."""
    env = (
        df.groupby("lvl_3")["val"]
        .sum()
        .sort_values(ascending=False)
        .to_dict()
    )
    return {
        "top_environments": {k: int(v) for k, v in list(env.items())[:15]},
        "total_unique": int(df["lvl_3"].nunique()),
    }


# ---------------------------------------------------------------------------
# 6. Metric distribution
# ---------------------------------------------------------------------------

def metric_analysis(df: pd.DataFrame) -> dict:
    """Val distribution per metric_name and platform."""
    by_metric = (
        df.groupby("metric_name")["val"]
        .sum()
        .sort_values(ascending=False)
        .to_dict()
    )
    by_platform = (
        df.groupby("event_category_name")["val"]
        .sum()
        .sort_values(ascending=False)
        .to_dict()
    )
    by_category = (
        df.groupby("lvl_1")["val"]
        .sum()
        .sort_values(ascending=False)
        .to_dict()
    )
    return {
        "by_metric": {k: int(v) for k, v in by_metric.items()},
        "by_platform": {k: int(v) for k, v in by_platform.items()},
        "by_category": {k: int(v) for k, v in by_category.items()},
    }


# ---------------------------------------------------------------------------
# 7. Workflow statistics
# ---------------------------------------------------------------------------

def workflow_analysis(df: pd.DataFrame) -> dict:
    """Top workflows by total val."""
    top = (
        df.groupby(["metric_name", "lvl_1", "lvl_2"])["val"]
        .sum()
        .sort_values(ascending=False)
        .head(20)
        .reset_index()
    )
    return {
        "top_workflows": top.to_dict("records"),
        "total_unique": int(df["lvl_2"].nunique()),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _trend_description(direction, slope, r2, p_value, pct_change) -> str:
    sig = "значимо" if p_value < 0.05 else "незначимо"
    arrows = {"growing": "↑", "declining": "↓", "stable": "→"}
    arrow = arrows.get(direction, "")
    if direction == "stable":
        return f"{arrow} Стабильно (R²={r2:.2f}, {sig})"
    sign = "+" if pct_change > 0 else ""
    return f"{arrow} {sign}{pct_change:.1f}% за период (slope={slope:+.1f}, R²={r2:.2f}, {sig})"
