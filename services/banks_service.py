from datetime import date, timedelta

import pandas as pd
from sqlalchemy import select

from database import SessionLocal, engine
from models import Banks

BANK_COLUMNS = ["bbva", "brg", "mp", "mp_liberar", "shop", "lvp", "coppel"]
EXPECTED_COLUMNS = ["date", *BANK_COLUMNS]
NUMERIC_COLUMNS = BANK_COLUMNS


def _last_completed_sunday() -> date:
    today = date.today()
    days_back = today.weekday() + 1 if today.weekday() != 6 else 7
    return today - timedelta(days=days_back)


def _upsert_banks_values(session, row_date, values: dict) -> tuple[int, int]:
    existing_row = session.query(Banks).filter(Banks.date == row_date).first()

    if existing_row:
        for col, value in values.items():
            setattr(existing_row, col, value)
        return 0, 1

    session.add(Banks(date=row_date, **values))
    return 1, 0


def upsert_banks_row(row_date, bbva=0, brg=0, mp=0, mp_liberar=0, shop=0, lvp=0, coppel=0):
    session = SessionLocal()
    values = dict(bbva=bbva, brg=brg, mp=mp, mp_liberar=mp_liberar, shop=shop, lvp=lvp, coppel=coppel)

    try:
        inserted, updated = _upsert_banks_values(session, row_date, values)
        session.commit()
        return inserted, updated
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_banks_history() -> pd.DataFrame:
    df = pd.read_sql(select(Banks), engine)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def get_latest_banks_date():
    df = get_banks_history()
    return None if df.empty else df["date"].max().date()


def get_missing_bank_week_end_dates_since_latest():
    latest_date = get_latest_banks_date()
    last_completed_week_end = _last_completed_sunday()

    if latest_date is None:
        return [last_completed_week_end] if last_completed_week_end < date.today() else []

    missing_dates = []
    current = latest_date + timedelta(days=7)
    while current <= last_completed_week_end:
        missing_dates.append(current)
        current += timedelta(days=7)
    return missing_dates


def get_banks_daily_totals() -> pd.DataFrame:
    df = get_banks_history()
    if df.empty:
        return pd.DataFrame(columns=["date", "banks_total"])

    df["banks_total"] = df[BANK_COLUMNS].sum(axis=1)
    return df[["date", "banks_total"]].sort_values("date").reset_index(drop=True)


def prepare_banks_dataframe(uploaded_file) -> pd.DataFrame:
    df = pd.read_csv(uploaded_file)
    df.columns = df.columns.str.strip()

    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in CSV: {missing_cols}")

    df = df[EXPECTED_COLUMNS].copy()
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="raise").dt.date
    df[NUMERIC_COLUMNS] = df[NUMERIC_COLUMNS].fillna(0)

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="raise").round(2)
    return df


def upsert_banks_from_dataframe(df: pd.DataFrame) -> tuple[int, int]:
    session = SessionLocal()
    inserted = updated = 0

    try:
        for _, row in df.iterrows():
            row_inserted, row_updated = _upsert_banks_values(
                session,
                row["date"],
                {col: row[col] for col in BANK_COLUMNS},
            )
            inserted += row_inserted
            updated += row_updated

        session.commit()
        return inserted, updated
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
