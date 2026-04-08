from sqlalchemy import select
import pandas as pd
from datetime import date, timedelta

from database import SessionLocal, engine
from models import Banks


BANK_COLUMNS = [
    "bbva",
    "brg",
    "mp",
    "mp_liberar",
    "shop",
    "lvp",
    "coppel",
]


def upsert_banks_row(
    row_date,
    bbva=0,
    brg=0,
    mp=0,
    mp_liberar=0,
    shop=0,
    lvp=0,
    coppel=0,
):
    session = SessionLocal()

    try:
        existing_row = session.query(Banks).filter(Banks.date == row_date).first()

        if existing_row:
            existing_row.bbva = bbva
            existing_row.brg = brg
            existing_row.mp = mp
            existing_row.mp_liberar = mp_liberar
            existing_row.shop = shop
            existing_row.lvp = lvp
            existing_row.coppel = coppel
            inserted = 0
            updated = 1
        else:
            new_row = Banks(
                date=row_date,
                bbva=bbva,
                brg=brg,
                mp=mp,
                mp_liberar=mp_liberar,
                shop=shop,
                lvp=lvp,
                coppel=coppel,
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


def get_latest_banks_date():
    df = get_banks_history()
    if df.empty:
        return None
    return df["date"].max().date()


def get_missing_bank_week_end_dates_since_latest():
    latest_date = get_latest_banks_date()
    today = date.today()

    # last completed Sunday
    last_completed_week_end = (
        today - timedelta(days=today.weekday() + 1)
        if today.weekday() != 6
        else today - timedelta(days=7)
    )

    if latest_date is None:
        return [last_completed_week_end] if last_completed_week_end < today else []

    missing_dates = []
    current = latest_date + timedelta(days=7)

    while current <= last_completed_week_end:
        missing_dates.append(current)
        current += timedelta(days=7)

    return missing_dates


def get_banks_history() -> pd.DataFrame:
    query = select(Banks)
    df = pd.read_sql(query, engine)

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])

    return df.sort_values("date").reset_index(drop=True)


def get_banks_daily_totals() -> pd.DataFrame:
    df = get_banks_history()

    if df.empty:
        return pd.DataFrame(columns=["date", "banks_total"])

    balance_cols = [
        "bbva",
        "brg",
        "mp",
        "mp_liberar",
        "shop",
        "lvp",
        "coppel",
    ]

    df["banks_total"] = df[balance_cols].sum(axis=1)

    return df[["date", "banks_total"]].sort_values("date").reset_index(drop=True)


EXPECTED_COLUMNS = [
    "date",
    "bbva",
    "brg",
    "mp",
    "mp_liberar",
    "shop",
    "lvp",
    "coppel",
]

NUMERIC_COLUMNS = [
    "bbva",
    "brg",
    "mp",
    "mp_liberar",
    "shop",
    "lvp",
    "coppel",
]


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
    inserted = 0
    updated = 0

    try:
        for _, row in df.iterrows():
            existing_row = session.query(Banks).filter(Banks.date == row["date"]).first()

            if existing_row:
                existing_row.bbva = row["bbva"]
                existing_row.brg = row["brg"]
                existing_row.mp = row["mp"]
                existing_row.mp_liberar = row["mp_liberar"]
                existing_row.shop = row["shop"]
                existing_row.lvp = row["lvp"]
                existing_row.coppel = row["coppel"]
                updated += 1
            else:
                new_row = Banks(
                    date=row["date"],
                    bbva=row["bbva"],
                    brg=row["brg"],
                    mp=row["mp"],
                    mp_liberar=row["mp_liberar"],
                    shop=row["shop"],
                    lvp=row["lvp"],
                    coppel=row["coppel"],
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