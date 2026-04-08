import pandas as pd
from datetime import date, timedelta
from sqlalchemy import select

from database import SessionLocal, engine
from models import AcquisitionExpense


EXPENSE_COLUMNS = [
    "Amazon",
    "Mercado_Libre",
    "Facebook",
    "Tiktok",
    "Google",
    "UGC_y_Colab",
    "Otros",
]


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

    try:
        existing_row = (
            session.query(AcquisitionExpense)
            .filter(AcquisitionExpense.date == row_date)
            .first()
        )

        if existing_row:
            existing_row.Amazon = Amazon
            existing_row.Mercado_Libre = Mercado_Libre
            existing_row.Facebook = Facebook
            existing_row.Tiktok = Tiktok
            existing_row.Google = Google
            existing_row.UGC_y_Colab = UGC_y_Colab
            existing_row.Otros = Otros
            updated = 1
            inserted = 0
        else:
            new_row = AcquisitionExpense(
                date=row_date,
                Amazon=Amazon,
                Mercado_Libre=Mercado_Libre,
                Facebook=Facebook,
                Tiktok=Tiktok,
                Google=Google,
                UGC_y_Colab=UGC_y_Colab,
                Otros=Otros,
            )
            session.add(new_row)
            inserted = 1
            updated = 0

        session.commit()
        return inserted, updated

    except Exception:
        session.rollback()
        raise

    finally:
        session.close()


def get_acquisition_expense_history() -> pd.DataFrame:
    query = select(AcquisitionExpense)
    df = pd.read_sql(query, engine)

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def get_latest_acquisition_expense_date():
    df = get_acquisition_expense_history()
    if df.empty:
        return None
    return df["date"].max().date()


def get_missing_week_end_dates_since_latest():
    latest_date = get_latest_acquisition_expense_date()
    today = date.today()

    # last completed week-end = last Sunday before today
    last_completed_week_end = today - timedelta(days=today.weekday() + 1) if today.weekday() != 6 else today - timedelta(days=7)

    if latest_date is None:
        return [last_completed_week_end] if last_completed_week_end < today else []

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

    for col in EXPENSE_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["acquisition_expense_total"] = df[EXPENSE_COLUMNS].sum(axis=1)

    return df[["date", "acquisition_expense_total"]].sort_values("date").reset_index(drop=True)