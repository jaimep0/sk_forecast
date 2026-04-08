import pandas as pd
from sqlalchemy import select

from database import SessionLocal, engine
from models import Units


def get_units_history() -> pd.DataFrame:
    query = select(Units)
    df = pd.read_sql(query, engine)

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])

    return df.sort_values(["date", "mkp_name"]).reset_index(drop=True)


def get_units_daily_totals() -> pd.DataFrame:
    df = get_units_history()

    if df.empty:
        return pd.DataFrame(columns=["date", "units_total"])

    product_cols = [
        "ipl_cl",
        "ipl_pro",
        "rasuradora",
        "perfiladora",
        "repuestos",
        "exfoliante",
        "agua_de_rosas",
    ]

    df["units_total"] = df[product_cols].sum(axis=1)

    return (
        df.groupby("date", as_index=False)["units_total"]
        .sum()
        .sort_values("date")
        .reset_index(drop=True)
    )


EXPECTED_COLUMNS = [
    "date",
    "mkp_name",
    "ipl_cl",
    "ipl_pro",
    "rasuradora",
    "perfiladora",
    "repuestos",
    "exfoliante",
    "agua_de_rosas",
]

NUMERIC_COLUMNS = [
    "ipl_cl",
    "ipl_pro",
    "rasuradora",
    "perfiladora",
    "repuestos",
    "exfoliante",
    "agua_de_rosas",
]


def prepare_units_dataframe(uploaded_file) -> pd.DataFrame:
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
        df[col] = df[col].astype(int)

    return df


def upsert_units_from_dataframe(df: pd.DataFrame) -> tuple[int, int]:
    session = SessionLocal()
    inserted = 0
    updated = 0

    try:
        for _, row in df.iterrows():
            existing_row = (
                session.query(Units)
                .filter(
                    Units.date == row["date"],
                    Units.mkp_name == row["mkp_name"],
                )
                .first()
            )

            if existing_row:
                existing_row.ipl_cl = row["ipl_cl"]
                existing_row.ipl_pro = row["ipl_pro"]
                existing_row.rasuradora = row["rasuradora"]
                existing_row.perfiladora = row["perfiladora"]
                existing_row.repuestos = row["repuestos"]
                existing_row.exfoliante = row["exfoliante"]
                existing_row.agua_de_rosas = row["agua_de_rosas"]
                updated += 1
            else:
                new_row = Units(
                    date=row["date"],
                    mkp_name=row["mkp_name"],
                    ipl_cl=row["ipl_cl"],
                    ipl_pro=row["ipl_pro"],
                    rasuradora=row["rasuradora"],
                    perfiladora=row["perfiladora"],
                    repuestos=row["repuestos"],
                    exfoliante=row["exfoliante"],
                    agua_de_rosas=row["agua_de_rosas"],
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