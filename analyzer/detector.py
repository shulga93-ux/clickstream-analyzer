import pandas as pd
import numpy as np
from scipy import stats
from sklearn.ensemble import IsolationForest
from typing import Optional


def detect_all(df: pd.DataFrame) -> dict:
    """Run all detection algorithms and return results."""
    results = {}

    # Choose time bucket based on data range
    hours = (df["timestamp"].max() - df["timestamp"].min()).total_seconds() / 3600
    if hours <= 2:
        freq = "1min"
    elif hours <= 24:
        freq = "5min"
    elif hours <= 168:
        freq = "1h"
    else:
        freq = "1D"

    results["freq"] = freq
    results["anomalies"] = detect_volume_anomalies(df, freq)
    results["trends"] = detect_trends(df, freq)
    results["deviations"] = detect_deviations(df, freq)
    results["user_anomalies"] = detect_user_anomalies(df)
    results["page_stats"] = page_analysis(df)
    results["event_stats"] = event_analysis(df)
    results["session_stats"] = session_analysis(df)

    return results


def detect_volume_anomalies(df: pd.DataFrame, freq: str = "1h") -> list:
    """Detect time buckets with abnormal event volume using Z-score."""
    ts = df.set_index("timestamp").resample(freq).size().reset_index()
    ts.columns = ["timestamp", "count"]

    if len(ts) < 5:
        return []

    mean = ts["count"].mean()
    std = ts["count"].std()
    if std == 0:
        return []

    ts["zscore"] = (ts["count"] - mean) / std
    anomalies = ts[ts["zscore"].abs() > 2.5].copy()

    result = []
    for _, row in anomalies.iterrows():
        direction = "spike" if row["zscore"] > 0 else "drop"
        result.append({
            "timestamp": row["timestamp"].isoformat(),
            "count": int(row["count"]),
            "zscore": round(float(row["zscore"]), 2),
            "direction": direction,
            "severity": "high" if abs(row["zscore"]) > 3.5 else "medium",
            "description": f"Traffic {direction}: {int(row['count'])} events (z={row['zscore']:.1f}, avg={mean:.1f})",
        })

    return result


def detect_trends(df: pd.DataFrame, freq: str = "1h") -> dict:
    """Detect overall trends using linear regression on time-bucketed volume."""
    ts = df.set_index("timestamp").resample(freq).size().reset_index()
    ts.columns = ["timestamp", "count"]

    if len(ts) < 3:
        return {"direction": "insufficient_data", "slope": 0, "r2": 0}

    x = np.arange(len(ts))
    y = ts["count"].values
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

    r2 = r_value ** 2
    if abs(slope) < 0.1 or r2 < 0.1:
        direction = "stable"
    elif slope > 0:
        direction = "growing"
    else:
        direction = "declining"

    # Moving average
    window = max(3, len(ts) // 5)
    ts["ma"] = ts["count"].rolling(window=window, min_periods=1).mean()

    # Hourly distribution
    hourly = df.groupby(df["timestamp"].dt.hour).size()

    # Peak hours
    peak_hours = hourly.nlargest(3).index.tolist()
    quiet_hours = hourly.nsmallest(3).index.tolist()

    return {
        "direction": direction,
        "slope": round(float(slope), 4),
        "r2": round(float(r2), 4),
        "p_value": round(float(p_value), 4),
        "is_significant": bool(p_value < 0.05),
        "peak_hours": peak_hours,
        "quiet_hours": quiet_hours,
        "timeseries": ts[["timestamp", "count", "ma"]].to_dict("records"),
        "description": _trend_description(direction, slope, r2, p_value),
    }


def detect_deviations(df: pd.DataFrame, freq: str = "1h") -> list:
    """Detect deviations from rolling average using IQR method."""
    ts = df.set_index("timestamp").resample(freq).size().reset_index()
    ts.columns = ["timestamp", "count"]

    if len(ts) < 7:
        return []

    window = max(5, len(ts) // 6)
    ts["rolling_mean"] = ts["count"].rolling(window=window, min_periods=3).mean()
    ts["rolling_std"] = ts["count"].rolling(window=window, min_periods=3).std()
    ts = ts.dropna()

    deviations = []
    for _, row in ts.iterrows():
        if row["rolling_std"] == 0:
            continue
        z = (row["count"] - row["rolling_mean"]) / row["rolling_std"]
        if abs(z) > 2.0:
            deviations.append({
                "timestamp": row["timestamp"].isoformat(),
                "count": int(row["count"]),
                "expected": round(float(row["rolling_mean"]), 1),
                "deviation_pct": round((row["count"] - row["rolling_mean"]) / row["rolling_mean"] * 100, 1),
                "zscore": round(float(z), 2),
                "direction": "above" if z > 0 else "below",
            })

    return deviations


def detect_user_anomalies(df: pd.DataFrame) -> dict:
    """Detect users with unusual behaviour using Isolation Forest."""
    if df["user_id"].nunique() < 5:
        return {"anomalous_users": [], "note": "Too few users for anomaly detection"}

    user_stats = df.groupby("user_id").agg(
        event_count=("timestamp", "count"),
        unique_pages=("page", "nunique"),
        unique_events=("event_type", "nunique"),
        session_count=("session_id", "nunique"),
    ).reset_index()

    features = user_stats[["event_count", "unique_pages", "unique_events", "session_count"]].fillna(0)

    if len(features) < 5:
        return {"anomalous_users": [], "note": "Too few users"}

    clf = IsolationForest(contamination=0.1, random_state=42)
    user_stats["anomaly"] = clf.fit_predict(features)
    user_stats["anomaly_score"] = clf.decision_function(features)

    anomalous = user_stats[user_stats["anomaly"] == -1].sort_values("anomaly_score")

    result = []
    for _, row in anomalous.iterrows():
        result.append({
            "user_id": str(row["user_id"]),
            "event_count": int(row["event_count"]),
            "unique_pages": int(row["unique_pages"]),
            "session_count": int(row["session_count"]),
            "anomaly_score": round(float(row["anomaly_score"]), 3),
        })

    return {
        "anomalous_users": result,
        "total_users": len(user_stats),
        "anomalous_count": len(result),
    }


def page_analysis(df: pd.DataFrame) -> dict:
    """Top pages and funnel-like stats."""
    page_counts = df.groupby("page").size().sort_values(ascending=False)
    top_pages = page_counts.head(10).to_dict()

    # Pages with high drop after visit (rough proxy)
    page_users = df.groupby("page")["user_id"].nunique().sort_values(ascending=False)

    return {
        "top_pages": top_pages,
        "top_pages_by_users": page_users.head(10).to_dict(),
        "total_unique_pages": int(page_counts.shape[0]),
    }


def event_analysis(df: pd.DataFrame) -> dict:
    """Event type distribution and trends."""
    event_counts = df["event_type"].value_counts().to_dict()

    # Event trend over time
    ts = df.groupby([pd.Grouper(key="timestamp", freq="1h"), "event_type"]).size().reset_index()
    ts.columns = ["timestamp", "event_type", "count"]

    return {
        "distribution": event_counts,
        "total_event_types": len(event_counts),
    }


def session_analysis(df: pd.DataFrame) -> dict:
    """Session length and depth stats."""
    session_stats = df.groupby("session_id").agg(
        events=("timestamp", "count"),
        pages=("page", "nunique"),
        duration_s=("timestamp", lambda x: (x.max() - x.min()).total_seconds()),
    )

    # Find outlier sessions
    q1 = session_stats["events"].quantile(0.25)
    q3 = session_stats["events"].quantile(0.75)
    iqr = q3 - q1
    outlier_sessions = session_stats[session_stats["events"] > q3 + 1.5 * iqr]

    return {
        "total_sessions": len(session_stats),
        "avg_events_per_session": round(float(session_stats["events"].mean()), 2),
        "median_events_per_session": round(float(session_stats["events"].median()), 2),
        "avg_duration_s": round(float(session_stats["duration_s"].mean()), 2),
        "median_duration_s": round(float(session_stats["duration_s"].median()), 2),
        "outlier_sessions_count": len(outlier_sessions),
        "max_events_session": int(session_stats["events"].max()),
    }


def _trend_description(direction, slope, r2, p_value) -> str:
    sig = "значимый" if p_value < 0.05 else "незначимый"
    if direction == "stable":
        return f"Трафик стабилен (R²={r2:.2f}, {sig})"
    elif direction == "growing":
        return f"Трафик растёт: +{slope:.2f} событий/период (R²={r2:.2f}, {sig})"
    else:
        return f"Трафик падает: {slope:.2f} событий/период (R²={r2:.2f}, {sig})"
