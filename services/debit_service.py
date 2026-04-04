import pandas as pd

from database import SessionLocal
from models import Debit


EXPECTED_COLUMNS = [
    "date",
    "bbva",
    "mp",
]

NUMERIC_COLUMNS = [
    "bbva",
    "mp",
]


def prepare_debit_dataframe(uploaded_file) -> pd.DataFrame:
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


def upsert_debit_from_dataframe(df: pd.DataFrame) -> tuple[int, int]:
    session = SessionLocal()
    inserted = 0
    updated = 0

    try:
        for _, row in df.iterrows():
            existing_row = session.query(Debit).filter(Debit.date == row["date"]).first()

            if existing_row:
                existing_row.bbva = row["bbva"]
                existing_row.mp = row["mp"]
                updated += 1
            else:
                new_row = Debit(
                    date=row["date"],
                    bbva=row["bbva"],
                    mp=row["mp"],
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