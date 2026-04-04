from sqlalchemy import select
import pandas as pd

from database import engine
from models import Banks


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