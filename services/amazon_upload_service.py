import pandas as pd

from services.sales_service import upsert_sales_from_dataframe
from services.units_service import upsert_units_from_dataframe

PRODUCT_COLUMNS = [
    "ipl_cl", "ipl_pro", "rasuradora", "perfiladora",
    "repuestos", "exfoliante", "agua_de_rosas",
]
DB_COLUMNS = ["date", "mkp_name", *PRODUCT_COLUMNS]
SKU_MAPPING = {
    "SS_BLANCO": "ipl_cl",
    "SS_ROSA": "ipl_cl",
    "D-IPLPRO-R": "ipl_pro",
    "D-IPLPRO-B": "ipl_pro",
    "RASURADORA-B": "rasuradora",
    "PERFILADORA-R": "perfiladora",
    "2PACKAGUADEROSAS": "agua_de_rosas",
    "2PACKEXFOLIANTE": "exfoliante",
}
PACK_SKUS = {"2PACKAGUADEROSAS", "2PACKEXFOLIANTE"}


def normalize_amz_sku(sku: str) -> str | None:
    return SKU_MAPPING.get(str(sku).strip().upper())


def _pivot_amazon(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    out = (
        df.pivot_table(
            index=["date", "mkp_name"],
            columns="product",
            values=value_col,
            aggfunc="sum",
            fill_value=0,
        )
        .reindex(columns=PRODUCT_COLUMNS, fill_value=0)
        .reset_index()
    )
    out.columns.name = None
    return out.reindex(columns=DB_COLUMNS, fill_value=0)


def amazon_by_day_from_file(file) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(file, sep="\t", encoding="utf-8", engine="python").copy()
    df["order-status"] = df["order-status"].astype(str).str.strip()
    df["sku"] = df["sku"].astype(str).str.strip().str.upper()
    df["product"] = df["sku"].apply(normalize_amz_sku)
    df = df[(df["order-status"] == "Shipped") & df["product"].notna()].copy()

    if df.empty:
        empty = pd.DataFrame(columns=DB_COLUMNS)
        return empty, empty.copy()

    df["date"] = pd.to_datetime(df["purchase-date"]).dt.date
    df["mkp_name"] = "amzn"
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    df["item-price"] = pd.to_numeric(df["item-price"], errors="coerce").fillna(0)
    df.loc[df["sku"].isin(PACK_SKUS), "quantity"] *= 2

    df_units = _pivot_amazon(df, "quantity")
    df_sales = _pivot_amazon(df, "item-price")

    df_units[PRODUCT_COLUMNS] = df_units[PRODUCT_COLUMNS].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)
    df_sales[PRODUCT_COLUMNS] = df_sales[PRODUCT_COLUMNS].apply(pd.to_numeric, errors="coerce").fillna(0).round(2)
    return df_units, df_sales


def amazon_by_day_from_files(files) -> tuple[pd.DataFrame, pd.DataFrame]:
    units_frames, sales_frames = [], []

    for file in files:
        df_units, df_sales = amazon_by_day_from_file(file)
        if not df_units.empty:
            units_frames.append(df_units)
        if not df_sales.empty:
            sales_frames.append(df_sales)

    if not units_frames:
        empty = pd.DataFrame(columns=DB_COLUMNS)
        return empty, empty.copy()

    grouped_units = (
        pd.concat(units_frames, ignore_index=True)
        .groupby(["date", "mkp_name"], as_index=False)[PRODUCT_COLUMNS].sum()
        .reindex(columns=DB_COLUMNS)
    )
    grouped_sales = (
        pd.concat(sales_frames, ignore_index=True)
        .groupby(["date", "mkp_name"], as_index=False)[PRODUCT_COLUMNS].sum()
        .reindex(columns=DB_COLUMNS)
    )
    return grouped_units, grouped_sales


def upload_amazon_files_to_db(files) -> dict:
    df_units, df_sales = amazon_by_day_from_files(files)
    units_inserted = units_updated = sales_inserted = sales_updated = 0

    if not df_units.empty:
        units_inserted, units_updated = upsert_units_from_dataframe(df_units)
    if not df_sales.empty:
        sales_inserted, sales_updated = upsert_sales_from_dataframe(df_sales)

    return {
        "units_rows": len(df_units),
        "sales_rows": len(df_sales),
        "units_inserted": units_inserted,
        "units_updated": units_updated,
        "sales_inserted": sales_inserted,
        "sales_updated": sales_updated,
        "df_units": df_units,
        "df_sales": df_sales,
    }
