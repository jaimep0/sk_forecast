import json
from datetime import date, timedelta

import pandas as pd
import requests
import os
from dotenv import load_dotenv
from settings import get_ml_creds

from services.units_service import upsert_units_from_dataframe
from services.sales_service import upsert_sales_from_dataframe

load_dotenv()

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


def refresh_access_token_mercado_libre():
    r = requests.post(
        "https://api.mercadolibre.com/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": os.getenv("ML_CLIENT_ID"),
            "client_secret": os.getenv("ML_CLIENT_SECRET"),
            "refresh_token": os.getenv("ML_REFRESH_TOKEN"),
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    data = r.json()

    if r.status_code == 200 and "access_token" in data:
        os.environ["ML_ACCESS_TOKEN"] = data["access_token"]

        if "refresh_token" in data:
            os.environ["ML_REFRESH_TOKEN"] = data["refresh_token"]

        print("Token refreshed successfully for current session.")
    else:
        raise RuntimeError(f"Error: {r.status_code} - {data}")


def normalize_ml_title(title: str) -> str | None:
    t = str(title).lower()

    if "repuesto" in t:
        return "repuestos"
    if "perfiladora" in t:
        return "perfiladora"
    if "rasuradora" in t or "eléctrica" in t:
        return "rasuradora"
    if "ipl pro" in t:
        return "ipl_pro"
    if "depiladora profesional" in t or "ipl shinnyskin" in t or "luz pulsada" in t:
        return "ipl_cl"
    if "agua" in t and "rosa" in t:
        return "agua_de_rosas"
    if "exfoliante" in t:
        return "exfoliante"

    return None


def ml_by_day(start_date: str, end_date: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    start_date_api = f"{start_date}T00:00:00.000Z"
    end_date_api = f"{end_date}T23:59:59.999Z"
    creds = get_ml_creds()

    base_url = "https://api.mercadolibre.com/orders/search"
    params = {
        "seller": creds["user_id"],
        "order.date_created.from": start_date_api,
        "order.date_created.to": end_date_api,
        "access_token": creds["access_token"],
        "limit": 50,
        "offset": 0,
    }

    orders = []

    while True:
        response = requests.get(base_url, params=params)

        if response.status_code != 200:
            refresh_access_token_mercado_libre()

            creds = get_ml_creds()

            params["access_token"] = creds["access_token"]
            response = requests.get(base_url, params=params)
            response.raise_for_status()

        data = response.json()
        results = data.get("results", [])

        if not results:
            break

        orders.extend(results)

        total = data.get("paging", {}).get("total", 0)
        if len(orders) >= total:
            break

        params["offset"] += params["limit"]

    if not orders:
        return pd.DataFrame(columns=DB_COLUMNS), pd.DataFrame(columns=DB_COLUMNS)

    rows = []

    for order in orders:
        if order.get("paid_amount", 0) <= 0:
            continue

        order_date = pd.to_datetime(order["date_created"]).date()

        for oi in order.get("order_items", []):
            item = oi.get("item", {})
            quantity = oi.get("quantity", 0) or 0

            unit_price = oi.get("unit_price")
            if unit_price is None:
                unit_price = oi.get("full_unit_price")

            if unit_price is None:
                line_amount = float(order.get("paid_amount", 0))
            else:
                line_amount = float(unit_price) * float(quantity)

            rows.append(
                {
                    "date": order_date,
                    "mkp_name": "ml",
                    "item_id": item.get("id"),
                    "raw_title": item.get("title"),
                    "product": normalize_ml_title(item.get("title")),
                    "quantity": quantity,
                    "amount": line_amount,
                }
            )

    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=DB_COLUMNS), pd.DataFrame(columns=DB_COLUMNS)

    df = df[df["product"].notna()].copy()

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
            values="amount",
            aggfunc="sum",
            fill_value=0,
        )
        .reindex(columns=product_columns, fill_value=0)
        .reset_index()
    )

    df_units.columns.name = None
    df_sales.columns.name = None

    df_units = df_units[DB_COLUMNS].copy()
    df_sales = df_sales[DB_COLUMNS].copy()

    for col in product_columns:
        df_units[col] = pd.to_numeric(df_units[col], errors="coerce").fillna(0).astype(int)
        df_sales[col] = pd.to_numeric(df_sales[col], errors="coerce").fillna(0).round(2)

    return df_units, df_sales


def update_ml_date_range(start_date: str, end_date: str) -> dict:
    df_units, df_sales = ml_by_day(start_date, end_date)

    units_inserted = units_updated = 0
    sales_inserted = sales_updated = 0

    if not df_units.empty:
        units_inserted, units_updated = upsert_units_from_dataframe(df_units)

    if not df_sales.empty:
        sales_inserted, sales_updated = upsert_sales_from_dataframe(df_sales)

    return {
        "start_date": start_date,
        "end_date": end_date,
        "units_rows": len(df_units),
        "sales_rows": len(df_sales),
        "units_inserted": units_inserted,
        "units_updated": units_updated,
        "sales_inserted": sales_inserted,
        "sales_updated": sales_updated,
    }


def update_ml_last_weeks(weeks: int) -> dict:
    if weeks < 1:
        raise ValueError("Weeks must be at least 1.")

    end_dt = date.today() - timedelta(days=1)
    start_dt = end_dt - timedelta(days=weeks * 7 - 1)

    return update_ml_date_range(
        start_date=start_dt.isoformat(),
        end_date=end_dt.isoformat(),
    )