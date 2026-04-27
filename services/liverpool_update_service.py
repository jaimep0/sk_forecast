import pandas as pd

from services.sales_service import upsert_sales_from_dataframe
from services.units_service import upsert_units_from_dataframe

PRODUCT_COLUMNS = [
    "ipl_cl", "ipl_pro", "rasuradora", "perfiladora",
    "repuestos", "exfoliante", "agua_de_rosas",
]
DB_COLUMNS = ["date", "mkp_name", *PRODUCT_COLUMNS]

PRODUCT_MAPPING = {
    "Depiladora IPL para rostro y cuerpo Shinnyskin": "ipl_cl",
    "Depiladora IPL para rostro y cuerpo Shinnyskin Pro": "ipl_pro",
    "Rasuradora corporal shinnyskin 4 en 1": "rasuradora",
}

EXCLUDED_STATUSES = {"Reembolsado", "Cancelado"}
DATE_COLUMNS = [
    "Fecha",
    "Fecha de creación",
    "Fecha de creacion",
    "Fecha de Creación",
    "Fecha de Creacion",
    "Fecha de venta",
    "Fecha de Venta",
    "Fecha del pedido",
    "Fecha del Pedido",
    "Fecha de compra",
    "Fecha de Compra",
    "Creado el",
]
REQUIRED_COLUMNS = [
    "Estado",
    "Nombre del producto",
    "Cantidad",
    "Subtotal de la linea a pagar al seller",
]


def normalize_liverpool_product(product_name: str) -> str | None:
    """Map Liverpool product names to the database product columns."""
    return PRODUCT_MAPPING.get(str(product_name).strip())


def _find_date_column(df: pd.DataFrame) -> str:
    for col in DATE_COLUMNS:
        if col in df.columns:
            return col
    raise ValueError(
        "Could not find a date column in the Liverpool CSV. "
        f"Expected one of: {DATE_COLUMNS}"
    )


def _validate_liverpool_columns(df: pd.DataFrame) -> None:
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in Liverpool CSV: {missing_cols}")


def _pivot_liverpool(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
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


def liverpool_by_day_from_file(file) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(file, sep=",", encoding="utf-8-sig", engine="python").copy()
    df.columns = df.columns.str.strip()

    _validate_liverpool_columns(df)
    date_col = _find_date_column(df)

    df["Estado"] = df["Estado"].astype(str).str.strip()
    df["Nombre del producto"] = df["Nombre del producto"].astype(str).str.strip()
    df["product"] = df["Nombre del producto"].apply(normalize_liverpool_product)

    df = df[
        (~df["Estado"].isin(EXCLUDED_STATUSES))
        & df["product"].notna()
    ].copy()

    if df.empty:
        empty = pd.DataFrame(columns=DB_COLUMNS)
        return empty, empty.copy()

    df["date"] = pd.to_datetime(df[date_col], dayfirst=True, errors="raise").dt.date
    df["mkp_name"] = "lvp"
    df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce").fillna(0)
    df["Subtotal de la linea a pagar al seller"] = pd.to_numeric(
        df["Subtotal de la linea a pagar al seller"],
        errors="coerce",
    ).fillna(0)

    df_units = _pivot_liverpool(df, "Cantidad")
    df_sales = _pivot_liverpool(df, "Subtotal de la linea a pagar al seller")

    df_units[PRODUCT_COLUMNS] = (
        df_units[PRODUCT_COLUMNS]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df_sales[PRODUCT_COLUMNS] = (
        df_sales[PRODUCT_COLUMNS]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
        .round(2)
    )

    return df_units, df_sales


def liverpool_by_day_from_files(files) -> tuple[pd.DataFrame, pd.DataFrame]:
    units_frames, sales_frames = [], []

    for file in files:
        df_units, df_sales = liverpool_by_day_from_file(file)
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


def upload_liverpool_files_to_db(files) -> dict:
    df_units, df_sales = liverpool_by_day_from_files(files)
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


# Optional helper equivalent to your original LP() idea.
def LP(file="lp.csv") -> pd.DataFrame:
    df_units, df_sales = liverpool_by_day_from_file(file)

    if df_units.empty and df_sales.empty:
        return pd.DataFrame(columns=["product", "units", "sales"])

    units = df_units[PRODUCT_COLUMNS].sum().rename("units")
    sales = df_sales[PRODUCT_COLUMNS].sum().rename("sales")

    return (
        pd.concat([units, sales], axis=1)
        .reset_index(names="product")
        .query("units != 0 or sales != 0")
        .reset_index(drop=True)
    )
