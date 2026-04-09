import pandas as pd
from pathlib import Path

# ─── Маппинги ────────────────────────────────────────────────────────────────

REQUIRED_COLS = {"report_dt", "val"}

METRIC_NAMES = {
    55556: "Тех Ошибка",
    55557: "Тех Ошибка (UnknownApp)",
    55558: "Статусный экран",
}

CHANNEL_NAMES = {
    "IPAD_SBOLPRO_platform.driver": "iPad (Планшеты)",
    "WEB_SBOLPRO_platform.driver": "Web (АРМ)",
}

SEGMENT_NAMES = {
    "EMP": "Сотрудники",
    "FL": "Физ Лица",
    "GB": "Гостевой Блок",
}

# Мусорные значения lvl_3
_JUNK_LVL3 = {"gf", "b12b"}


def get_block_type(lvl_1: str, lvl_3: str) -> str:
    """Определяет тип блока (пилотный/боевой/резервный/неизвестный)
    по сегменту (lvl_1) и окружению (lvl_3).
    """
    seg = str(lvl_1).strip().upper()
    env = str(lvl_3).strip().lower()

    if seg == "EMP":
        if env == "sandbox":
            return "пилотный"
        elif env in ("greenfield", "bluefield"):
            return "боевой"

    elif seg == "FL":
        if env == "greenfield":
            return "пилотный"
        elif env.startswith("b") and env[1:].isdigit() and 1 <= int(env[1:]) <= 8:
            return "боевой"
        elif env.startswith("si") and env[2:].isdigit() and 1 <= int(env[2:]) <= 4:
            return "резервный"

    elif seg == "GB":
        if env == "b5":
            return "пилотный"
        elif env in ("b3", "b4"):
            return "боевой"
        elif env in ("si3", "si4"):
            return "резервный"

    return "неизвестный"


def _is_junk_lvl3(val: str) -> bool:
    """Возвращает True если значение lvl_3 — мусорное."""
    v = str(val).strip()
    return v in _JUNK_LVL3 or v.startswith("http")


# ─── Парсинг ─────────────────────────────────────────────────────────────────

def parse_file(filepath: str) -> pd.DataFrame:
    """Parse a CSV or XLSX metrics file into a normalized DataFrame."""
    path = Path(filepath)
    ext = path.suffix.lower()

    if ext == ".csv":
        df = pd.read_csv(filepath)
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(filepath, engine="openpyxl")
    else:
        raise ValueError(f"Unsupported format: {ext}. Expected .csv or .xlsx")

    df = _normalize(df)
    return df


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names, types, add derived columns, filter junk rows."""
    # Lowercase column names
    df.columns = [c.strip().lower() for c in df.columns]

    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns: {missing}. "
            f"Expected: metric_id, period_type, report_dt, event_category_name, "
            f"log_name, lvl_1, lvl_2, lvl_3, lvl_4, val"
        )

    # Parse date
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
            df[col] = df[col].fillna("unknown").astype(str).str.strip()

    # ── Фильтрация мусорных lvl_3 ──
    df = df[~df["lvl_3"].apply(_is_junk_lvl3)].reset_index(drop=True)

    # ── Производные колонки ──
    # metric_id как число для маппинга
    df["metric_id_int"] = pd.to_numeric(df["metric_id"], errors="coerce")

    # metric_name
    df["metric_name"] = df["metric_id_int"].map(METRIC_NAMES).fillna(df["metric_id"])

    # channel_name
    df["channel_name"] = df["event_category_name"].map(CHANNEL_NAMES).fillna(df["event_category_name"])

    # segment_name
    df["segment_name"] = df["lvl_1"].map(SEGMENT_NAMES).fillna(df["lvl_1"])

    # block_type
    df["block_type"] = df.apply(lambda r: get_block_type(r["lvl_1"], r["lvl_3"]), axis=1)

    # Convenience alias
    df["timestamp"] = df["report_dt"]

    return df


# ─── Summary ─────────────────────────────────────────────────────────────────

def get_summary(df: pd.DataFrame) -> dict:
    """Return basic stats about the dataset."""
    dates = sorted(df["report_dt"].dt.date.unique())

    val_by_date = (
        df.groupby(df["report_dt"].dt.date)["val"]
        .sum()
        .to_dict()
    )
    val_by_date = {str(k): int(v) for k, v in val_by_date.items()}

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
        "unique_channels": int(df["channel_name"].nunique()),
        "unique_segments": int(df["segment_name"].nunique()),
        "unique_products": int(df["lvl_2"].nunique()),
        "unique_services": int(df["log_name"].nunique()),
        "total_val": int(df["val"].sum()),
        "val_by_date": val_by_date,
        "val_by_metric": val_by_metric,
        # backward-compat aliases
        "unique_workflows": int(df["lvl_2"].nunique()),
        "unique_environments": int(df["lvl_3"].nunique()),
        "metrics": df["metric_name"].unique().tolist(),
        "columns": list(df.columns),
    }
