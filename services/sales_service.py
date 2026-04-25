import pandas as pd
from sqlalchemy import select

from database import SessionLocal, engine
from models import Sales

PRODUCT_COLUMNS = [
    "ipl_cl", "ipl_pro", "rasuradora", "perfiladora",
    "repuestos", "exfoliante", "agua_de_rosas",
]
EXPECTED_COLUMNS = ["date", "mkp_name", *PRODUCT_COLUMNS]
NUMERIC_COLUMNS = PRODUCT_COLUMNS


def get_sales_history() -> pd.DataFrame:
    df = pd.read_sql(select(Sales), engine)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["date", "mkp_name"]).reset_index(drop=True)


def get_sales_daily_totals() -> pd.DataFrame:
    df = get_sales_history()
    if df.empty:
        return pd.DataFrame(columns=["date", "sales_total"])

    return (
        df.assign(sales_total=df[PRODUCT_COLUMNS].sum(axis=1))
        .groupby("date", as_index=False)["sales_total"].sum()
        .sort_values("date")
        .reset_index(drop=True)
    )


def get_sales_weekly_totals() -> pd.DataFrame:
    df = get_sales_daily_totals()
    if df.empty:
        return pd.DataFrame(columns=["date", "sales_total"])

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["week_end"] = df["date"] + pd.to_timedelta(6 - df["date"].dt.weekday, unit="D")

    return (
        df.groupby("week_end", as_index=False)["sales_total"].sum()
        .rename(columns={"week_end": "date"})
        .sort_values("date")
        .reset_index(drop=True)
    )


def prepare_sales_dataframe(uploaded_file) -> pd.DataFrame:
    df = pd.read_csv(uploaded_file)
    df.columns = df.columns.str.strip()

    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in CSV: {missing_cols}")

    df = df[EXPECTED_COLUMNS].copy()
    df["mkp_name"] = df["mkp_name"].astype(str).str.strip().str.lower()
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="raise").dt.date
    df[NUMERIC_COLUMNS] = df[NUMERIC_COLUMNS].fillna(0)

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="raise").round(2)
    return df


def upsert_sales_from_dataframe(df: pd.DataFrame) -> tuple[int, int]:
    session = SessionLocal()
    inserted = updated = 0

    try:
        for _, row in df.iterrows():
            existing_row = (
                session.query(Sales)
                .filter(Sales.date == row["date"], Sales.mkp_name == row["mkp_name"])
                .first()
            )

            values = {col: row[col] for col in PRODUCT_COLUMNS}
            if existing_row:
                for col, value in values.items():
                    setattr(existing_row, col, value)
                updated += 1
            else:
                session.add(Sales(date=row["date"], mkp_name=row["mkp_name"], **values))
                inserted += 1

        session.commit()
        return inserted, updated
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
