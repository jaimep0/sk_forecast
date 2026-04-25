from datetime import date, timedelta

import pandas as pd
from sqlalchemy import select

from database import SessionLocal, engine
from models import AcquisitionExpense

EXPENSE_COLUMNS = [
    "Amazon", "Mercado_Libre", "Facebook", "Tiktok",
    "Google", "UGC_y_Colab", "Otros",
]


def _last_completed_sunday() -> date:
    today = date.today()
    days_back = today.weekday() + 1 if today.weekday() != 6 else 7
    return today - timedelta(days=days_back)


def upsert_acquisition_expense_row(
    row_date,
    Amazon=0,
    Mercado_Libre=0,
    Facebook=0,
    Tiktok=0,
    Google=0,
    UGC_y_Colab=0,
    Otros=0,
):
    session = SessionLocal()
    values = dict(
        Amazon=Amazon,
        Mercado_Libre=Mercado_Libre,
        Facebook=Facebook,
        Tiktok=Tiktok,
        Google=Google,
        UGC_y_Colab=UGC_y_Colab,
        Otros=Otros,
    )

    try:
        existing_row = (
            session.query(AcquisitionExpense)
            .filter(AcquisitionExpense.date == row_date)
            .first()
        )

        if existing_row:
            for col, value in values.items():
                setattr(existing_row, col, value)
            inserted, updated = 0, 1
        else:
            session.add(AcquisitionExpense(date=row_date, **values))
            inserted, updated = 1, 0

        session.commit()
        return inserted, updated
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_acquisition_expense_history() -> pd.DataFrame:
    df = pd.read_sql(select(AcquisitionExpense), engine)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def get_latest_acquisition_expense_date():
    df = get_acquisition_expense_history()
    return None if df.empty else df["date"].max().date()


def get_missing_week_end_dates_since_latest():
    latest_date = get_latest_acquisition_expense_date()
    last_completed_week_end = _last_completed_sunday()

    if latest_date is None:
        return [last_completed_week_end] if last_completed_week_end < date.today() else []

    missing_dates = []
    current = latest_date + timedelta(days=7)
    while current <= last_completed_week_end:
        missing_dates.append(current)
        current += timedelta(days=7)
    return missing_dates


def get_acquisition_expense_daily_totals() -> pd.DataFrame:
    df = get_acquisition_expense_history()
    if df.empty:
        return pd.DataFrame(columns=["date", "acquisition_expense_total"])

    df[EXPENSE_COLUMNS] = df[EXPENSE_COLUMNS].apply(pd.to_numeric, errors="coerce").fillna(0)
    df["acquisition_expense_total"] = df[EXPENSE_COLUMNS].sum(axis=1)
    return df[["date", "acquisition_expense_total"]].sort_values("date").reset_index(drop=True)
