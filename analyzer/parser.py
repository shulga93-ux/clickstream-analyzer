import pandas as pd
from pathlib import Path

# Expected columns in the metrics CSV format
REQUIRED_COLS = {"report_dt", "val"}
EXPECTED_COLS = {
    "metric_id", "period_type", "report_dt",
    "event_category_name", "log_name",
    "lvl_1", "lvl_2", "lvl_3", "lvl_4", "val",
}

METRIC_NAMES = {
    55556: "Ошибки Workflow",
    55557: "unknown.app",
    55558: "Статусный экран",
}


def parse_file(filepath: str) -> pd.DataFrame:
    """Parse a metrics CSV file into a normalized DataFrame."""
    path = Path(filepath)
    ext = path.suffix.lower()

    if ext != ".csv":
        raise ValueError(f"Unsupported format: {ext}. Expected .csv")

    df = pd.read_csv(filepath)
    df = _normalize(df)
    return df


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and types for the metrics format."""
    # Lowercase and strip column names
    df.columns = [c.strip().lower() for c in df.columns]

    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns: {missing}. "
            f"Expected format: metric_id, period_type, report_dt, "
            f"event_category_name, log_name, lvl_1, lvl_2, lvl_3, lvl_4, val"
        )

    # Parse date column
    df["report_dt"] = pd.to_datetime(df["report_dt"], errors="coerce")
    df = df.dropna(subset=["report_dt"])
    df = df.sort_values("report_dt").reset_index(drop=True)

    # Numeric val
    df["val"] = pd.to_numeric(df["val"], errors="coerce").fillna(0).astype(int)

    # Fill optional string columns
    for col in ["metric_id", "period_type", "event_category_name",
                "log_name", "lvl_1", "lvl_2", "lvl_3", "lvl_4"]:
        if col not in df.columns:
            df[col] = "unknown"
        else:
            df[col] = df[col].fillna("unknown").astype(str)

    # Add human-readable metric name
    if "metric_id" in df.columns:
        df["metric_id_int"] = pd.to_numeric(df["metric_id"], errors="coerce")
        df["metric_name"] = df["metric_id_int"].map(METRIC_NAMES).fillna(df["metric_id"])

    # Convenience alias: timestamp = report_dt (keeps detector compatible)
    df["timestamp"] = df["report_dt"]

    return df


def get_summary(df: pd.DataFrame) -> dict:
    """Return basic stats about the metrics dataset."""
    dates = sorted(df["report_dt"].dt.date.unique())

    # Val statistics
    val_stats = df["val"].describe()

    # Total val per date
    val_by_date = (
        df.groupby(df["report_dt"].dt.date)["val"]
        .sum()
        .to_dict()
    )
    val_by_date = {str(k): int(v) for k, v in val_by_date.items()}

    # Per-metric totals
    val_by_metric = (
        df.groupby("metric_name")["val"]
        .sum()
        .sort_values(ascending=False)
        .to_dict()
    )
    val_by_metric = {k: int(v) for k, v in val_by_metric.items()}

    return {
        "total_records": len(df),
        "unique_dates": len(dates),
        "date_range": {
            "start": str(dates[0]) if dates else "",
            "end": str(dates[-1]) if dates else "",
        },
        "unique_metrics": int(df["metric_id"].nunique()),
        "unique_platforms": int(df["event_category_name"].nunique()),
        "unique_workflows": int(df["lvl_2"].nunique()),
        "unique_environments": int(df["lvl_3"].nunique()),
        "total_val": int(df["val"].sum()),
        "val_mean": round(float(val_stats["mean"]), 2),
        "val_median": round(float(df["val"].median()), 2),
        "val_max": int(val_stats["max"]),
        "val_by_date": val_by_date,
        "val_by_metric": val_by_metric,
        "platforms": df["event_category_name"].unique().tolist(),
        "metrics": df["metric_name"].unique().tolist(),
        "columns": list(df.columns),
    }
