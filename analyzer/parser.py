import pandas as pd
import json
import io
from pathlib import Path


REQUIRED_COLS = {"timestamp"}
OPTIONAL_COLS = {"user_id", "session_id", "event_type", "page", "url", "duration", "value"}


def parse_file(filepath: str) -> pd.DataFrame:
    """Parse a clickstream file (CSV or JSONL) into a normalized DataFrame."""
    path = Path(filepath)
    ext = path.suffix.lower()

    if ext == ".csv":
        df = pd.read_csv(filepath)
    elif ext in (".json", ".jsonl", ".ndjson"):
        with open(filepath) as f:
            content = f.read().strip()
        if content.startswith("["):
            df = pd.read_json(io.StringIO(content))
        else:
            lines = [json.loads(l) for l in content.splitlines() if l.strip()]
            df = pd.DataFrame(lines)
    else:
        raise ValueError(f"Unsupported file format: {ext}. Use CSV or JSON/JSONL.")

    df = _normalize(df)
    return df


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and parse timestamps."""
    # Lowercase and strip column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Aliases
    aliases = {
        "time": "timestamp", "ts": "timestamp", "date": "timestamp",
        "datetime": "timestamp", "event_time": "timestamp",
        "userid": "user_id", "uid": "user_id", "client_id": "user_id",
        "sessionid": "session_id", "sid": "session_id",
        "event": "event_type", "action": "event_type", "type": "event_type",
        "path": "page", "uri": "page", "endpoint": "page",
    }
    df = df.rename(columns={k: v for k, v in aliases.items() if k in df.columns})

    if "timestamp" not in df.columns:
        raise ValueError("No timestamp column found. Expected: timestamp, time, ts, date, datetime, event_time")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False, errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Fill missing optional cols
    for col in ["user_id", "session_id", "event_type", "page"]:
        if col not in df.columns:
            df[col] = "unknown"

    if "duration" not in df.columns:
        df["duration"] = None

    return df


def get_summary(df: pd.DataFrame) -> dict:
    """Return basic stats about the dataset."""
    return {
        "total_events": len(df),
        "unique_users": df["user_id"].nunique(),
        "unique_sessions": df["session_id"].nunique(),
        "unique_pages": df["page"].nunique(),
        "event_types": df["event_type"].value_counts().to_dict(),
        "time_range": {
            "start": df["timestamp"].min().isoformat(),
            "end": df["timestamp"].max().isoformat(),
            "duration_hours": round((df["timestamp"].max() - df["timestamp"].min()).total_seconds() / 3600, 2),
        },
        "columns": list(df.columns),
    }
