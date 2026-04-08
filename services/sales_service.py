import pandas as pd
from sqlalchemy import select

from database import SessionLocal, engine
from models import Sales


def get_sales_weekly_totals() -> pd.DataFrame:
    df = get_sales_daily_totals()

    if df.empty:
        return pd.DataFrame(columns=["date", "sales_total"])

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    # weekly bucket ending on Sunday
    df["week_end"] = df["date"] + pd.to_timedelta(6 - df["date"].dt.weekday, unit="D")

    weekly_df = (
        df.groupby("week_end", as_index=False)["sales_total"]
        .sum()
        .rename(columns={"week_end": "date"})
        .sort_values("date")
        .reset_index(drop=True)
    )

    return weekly_df


def get_sales_history() -> pd.DataFrame:
    query = select(Sales)
    df = pd.read_sql(query, engine)

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])

    return df.sort_values(["date", "mkp_name"]).reset_index(drop=True)


def get_sales_daily_totals() -> pd.DataFrame:
    df = get_sales_history()

    if df.empty:
        return pd.DataFrame(columns=["date", "sales_total"])

    product_cols = [
        "ipl_cl",
        "ipl_pro",
        "rasuradora",
        "perfiladora",
        "repuestos",
        "exfoliante",
        "agua_de_rosas",
    ]

    df["sales_total"] = df[product_cols].sum(axis=1)

    return (
        df.groupby("date", as_index=False)["sales_total"]
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
    inserted = 0
    updated = 0

    try:
        for _, row in df.iterrows():
            existing_row = (
                session.query(Sales)
                .filter(
                    Sales.date == row["date"],
                    Sales.mkp_name == row["mkp_name"],
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
                new_row = Sales(
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