import pandas as pd
from sqlalchemy import select

from database import SessionLocal, engine
from models import Units

PRODUCT_COLUMNS = [
    "ipl_cl", "ipl_pro", "rasuradora", "perfiladora",
    "repuestos", "exfoliante", "agua_de_rosas",
]
EXPECTED_COLUMNS = ["date", "mkp_name", *PRODUCT_COLUMNS]
NUMERIC_COLUMNS = PRODUCT_COLUMNS


def get_units_history() -> pd.DataFrame:
    df = pd.read_sql(select(Units), engine)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["date", "mkp_name"]).reset_index(drop=True)


def get_units_daily_totals() -> pd.DataFrame:
    df = get_units_history()
    if df.empty:
        return pd.DataFrame(columns=["date", "units_total"])

    return (
        df.assign(units_total=df[PRODUCT_COLUMNS].sum(axis=1))
        .groupby("date", as_index=False)["units_total"].sum()
        .sort_values("date")
        .reset_index(drop=True)
    )


def prepare_units_dataframe(uploaded_file) -> pd.DataFrame:
    df = pd.read_csv(uploaded_file)
    df.columns = df.columns.str.strip()

    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in CSV: {missing_cols}")

    df = df[EXPECTED_COLUMNS].copy()
    df["mkp_name"] = df["mkp_name"].astype(str).str.strip().str.lower()
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="raise").dt.date
    df[NUMERIC_COLUMNS] = df[NUMERIC_COLUMNS].fillna(0).astype(int)
    return df


def upsert_units_from_dataframe(df: pd.DataFrame) -> tuple[int, int]:
    session = SessionLocal()
    inserted = updated = 0

    try:
        for _, row in df.iterrows():
            existing_row = (
                session.query(Units)
                .filter(Units.date == row["date"], Units.mkp_name == row["mkp_name"])
                .first()
            )

            values = {col: row[col] for col in PRODUCT_COLUMNS}
            if existing_row:
                for col, value in values.items():
                    setattr(existing_row, col, value)
                updated += 1
            else:
                session.add(Units(date=row["date"], mkp_name=row["mkp_name"], **values))
                inserted += 1

        session.commit()
        return inserted, updated
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
