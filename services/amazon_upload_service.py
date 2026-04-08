import pandas as pd

from services.units_service import upsert_units_from_dataframe
from services.sales_service import upsert_sales_from_dataframe


DB_COLUMNS = [
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


def normalize_amz_sku(sku: str) -> str | None:
    sku = str(sku).strip().upper()

    mapping = {
        "SS_BLANCO": "ipl_cl",
        "SS_ROSA": "ipl_cl",
        "D-IPLPRO-R": "ipl_pro",
        "D-IPLPRO-B": "ipl_pro",
        "RASURADORA-B": "rasuradora",
        "PERFILADORA-R": "perfiladora",
        "2PACKAGUADEROSAS": "agua_de_rosas",
        "2PACKEXFOLIANTE": "exfoliante",
        # "REPUESTOS-XYZ": "repuestos",
    }

    return mapping.get(sku)


def amazon_by_day_from_file(file) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(
        file,
        sep="\t",
        encoding="utf-8",
        engine="python",
    )

    df = df.copy()

    df["order-status"] = df["order-status"].astype(str).str.strip()
    df["sku"] = df["sku"].astype(str).str.strip().str.upper()
    df["product"] = df["sku"].apply(normalize_amz_sku)

    df = df[
        (df["order-status"] == "Shipped") &
        (df["product"].notna())
    ].copy()

    if df.empty:
        return pd.DataFrame(columns=DB_COLUMNS), pd.DataFrame(columns=DB_COLUMNS)

    df["date"] = pd.to_datetime(df["purchase-date"]).dt.date
    df["mkp_name"] = "amzn"

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    df["item-price"] = pd.to_numeric(df["item-price"], errors="coerce").fillna(0)

    pack_mask = df["sku"].isin(["2PACKAGUADEROSAS", "2PACKEXFOLIANTE"])
    df.loc[pack_mask, "quantity"] = df.loc[pack_mask, "quantity"] * 2
    df.loc[pack_mask, "item-price"] = df.loc[pack_mask, "item-price"]

    product_columns = [
        "ipl_cl",
        "ipl_pro",
        "rasuradora",
        "perfiladora",
        "repuestos",
        "exfoliante",
        "agua_de_rosas",
    ]

    df_units = (
        df.pivot_table(
            index=["date", "mkp_name"],
            columns="product",
            values="quantity",
            aggfunc="sum",
            fill_value=0,
        )
        .reindex(columns=product_columns, fill_value=0)
        .reset_index()
    )

    df_sales = (
        df.pivot_table(
            index=["date", "mkp_name"],
            columns="product",
            values="item-price",
            aggfunc="sum",
            fill_value=0,
        )
        .reindex(columns=product_columns, fill_value=0)
        .reset_index()
    )

    df_units.columns.name = None
    df_sales.columns.name = None

    df_units = df_units.reindex(columns=DB_COLUMNS, fill_value=0)
    df_sales = df_sales.reindex(columns=DB_COLUMNS, fill_value=0)

    for col in product_columns:
        df_units[col] = pd.to_numeric(df_units[col], errors="coerce").fillna(0).astype(int)
        df_sales[col] = pd.to_numeric(df_sales[col], errors="coerce").fillna(0).round(2)

    return df_units, df_sales


def amazon_by_day_from_files(files) -> tuple[pd.DataFrame, pd.DataFrame]:
    units_frames = []
    sales_frames = []

    for file in files:
        df_units, df_sales = amazon_by_day_from_file(file)

        if not df_units.empty:
            units_frames.append(df_units)

        if not df_sales.empty:
            sales_frames.append(df_sales)

    if not units_frames:
        empty_units = pd.DataFrame(columns=DB_COLUMNS)
        empty_sales = pd.DataFrame(columns=DB_COLUMNS)
        return empty_units, empty_sales

    all_units = pd.concat(units_frames, ignore_index=True)
    all_sales = pd.concat(sales_frames, ignore_index=True)

    product_columns = [
        "ipl_cl",
        "ipl_pro",
        "rasuradora",
        "perfiladora",
        "repuestos",
        "exfoliante",
        "agua_de_rosas",
    ]

    grouped_units = (
        all_units.groupby(["date", "mkp_name"], as_index=False)[product_columns]
        .sum()
        .reindex(columns=DB_COLUMNS)
    )

    grouped_sales = (
        all_sales.groupby(["date", "mkp_name"], as_index=False)[product_columns]
        .sum()
        .reindex(columns=DB_COLUMNS)
    )

    return grouped_units, grouped_sales


def upload_amazon_files_to_db(files) -> dict:
    df_units, df_sales = amazon_by_day_from_files(files)

    units_inserted = units_updated = 0
    sales_inserted = sales_updated = 0

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