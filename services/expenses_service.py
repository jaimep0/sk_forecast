import pandas as pd
from sqlalchemy import select

from database import SessionLocal, engine
from models import Expenses


EXPECTED_COLUMNS = [
    "date",
    "concept",
    "total",
]

NUMERIC_COLUMNS = [
    "total",
]


def prepare_expenses_dataframe(uploaded_file) -> pd.DataFrame:
    df = pd.read_csv(uploaded_file)
    df.columns = df.columns.str.strip()

    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in CSV: {missing_cols}")

    df = df[EXPECTED_COLUMNS].copy()
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="raise").dt.date
    df["concept"] = df["concept"].astype(str).str.strip()
    df[NUMERIC_COLUMNS] = df[NUMERIC_COLUMNS].fillna(0)

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="raise").round(2)

    return df


def upsert_expenses_from_dataframe(df: pd.DataFrame) -> tuple[int, int]:
    session = SessionLocal()
    inserted = 0
    updated = 0

    try:
        for _, row in df.iterrows():
            existing_row = (
                session.query(Expenses)
                .filter(
                    Expenses.date == row["date"],
                    Expenses.concept == row["concept"],
                )
                .first()
            )

            if existing_row:
                existing_row.total = row["total"]
                updated += 1
            else:
                new_row = Expenses(
                    date=row["date"],
                    concept=row["concept"],
                    total=row["total"],
                )
                session.add(new_row)
                inserted += 1

        session.commit()
        return inserted, updated

    except Exception:
        session.rollback()
        raise

    finally:
        session.close()


def get_expenses_history() -> pd.DataFrame:
    query = select(Expenses)
    df = pd.read_sql(query, engine)

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["date", "concept"]).reset_index(drop=True)


def get_expenses_daily_totals() -> pd.DataFrame:
    df = get_expenses_history()

    if df.empty:
        return pd.DataFrame(columns=["date", "expenses_total"])

    return (
        df.groupby("date", as_index=False)["total"]
        .sum()
        .rename(columns={"total": "expenses_total"})
        .sort_values("date")
        .reset_index(drop=True)
    )