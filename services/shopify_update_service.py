import json
import os
import unicodedata
from datetime import date, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv

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

PRODUCT_COLUMNS = [
    "ipl_cl",
    "ipl_pro",
    "rasuradora",
    "perfiladora",
    "repuestos",
    "exfoliante",
    "agua_de_rosas",
]

SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-04")
SHOPIFY_TIMEZONE = os.getenv("SHOPIFY_TIMEZONE", "America/Mexico_City")
SHOPIFY_MARKETPLACE_NAME = os.getenv("SHOPIFY_MARKETPLACE_NAME", "shopify")


def get_shopify_creds() -> dict:
    """
    Preferred environment variables:
        SHOPIFY_DOMAIN=your-glowy-skin.myshopify.com
        SHOPIFY_API_TOKEN=shpat_...

    Optional local fallback for development:
        creds.json
        {
            "SHOP": {
                "domain": "your-glowy-skin.myshopify.com",
                "api_token": "shpat_..."
            }
        }
    """
    domain = os.getenv("SHOPIFY_DOMAIN")
    api_token = os.getenv("SHOPIFY_API_TOKEN")

    if domain and api_token:
        return {
            "domain": domain.replace("https://", "").replace("http://", "").strip("/"),
            "api_token": api_token,
        }

    if os.path.exists("creds.json"):
        with open("creds.json", encoding="utf-8") as f:
            creds = json.load(f)
        shop_creds = creds.get("SHOP", {})
        domain = shop_creds.get("domain") or shop_creds.get("shop_domain") or "your-glowy-skin.myshopify.com"
        api_token = shop_creds.get("api_token")
        if api_token:
            return {
                "domain": domain.replace("https://", "").replace("http://", "").strip("/"),
                "api_token": api_token,
            }

    raise RuntimeError(
        "Missing Shopify credentials. Add SHOPIFY_DOMAIN and SHOPIFY_API_TOKEN to your .env file."
    )


def _strip_accents(value: str) -> str:
    value = unicodedata.normalize("NFKD", str(value))
    return "".join(char for char in value if not unicodedata.combining(char))


def normalize_shopify_title(title) -> list[str]:
    """
    Returns all product columns affected by one Shopify line item.
    Kits intentionally return multiple products.
    """
    t = _strip_accents(str(title).lower())

    # Kits first, because a kit can include words from several products.
    if "kit pro" in t and "depilacion permanente" in t:
        return ["ipl_pro", "exfoliante", "agua_de_rosas"]

    if "kit clasico" in t and "depilacion permanente" in t:
        return ["ipl_cl", "exfoliante", "agua_de_rosas"]

    # Repuestos before generic IPL rules, because some replacement titles can contain IPL/device words.
    if "repuesto" in t or "repuestos" in t or "cartucho" in t or "cartuchos" in t:
        return ["repuestos"]

    if "perfiladora" in t:
        return ["perfiladora"]

    if "rasuradora" in t:
        return ["rasuradora"]

    if "exfoliante" in t:
        return ["exfoliante"]

    if "agua de rosas" in t or "tonico post-depilacion" in t or "tonico post depilacion" in t:
        return ["agua_de_rosas"]

    if "depiladora permanente ipl pro" in t or "ipl pro" in t:
        return ["ipl_pro"]

    if "depiladora permanente ipl clasica" in t:
        return ["ipl_cl"]

    if "depiladora" in t and "pro" not in t:
        return ["ipl_cl"]

    return []


def extract_quantity(item: dict) -> int:
    if not isinstance(item, dict):
        return 0
    return int(item.get("quantity", 0) or 0)


def extract_line_amount(item: dict) -> float:
    """
    Shopify line_items price is unit price.
    Real line sales = price * quantity - discount_allocations.
    """
    if not isinstance(item, dict):
        return 0.0

    quantity = float(item.get("quantity", 0) or 0)
    price = float(item.get("price", 0) or 0)
    gross_amount = price * quantity

    discount_amount = 0.0
    discount_allocations = item.get("discount_allocations", [])
    if isinstance(discount_allocations, list):
        discount_amount = sum(
            float(discount.get("amount", 0) or 0)
            for discount in discount_allocations
            if isinstance(discount, dict)
        )

    return max(gross_amount - discount_amount, 0.0)


def get_next_page_url(link_header: str | None) -> str | None:
    if not link_header:
        return None

    for link in link_header.split(","):
        if 'rel="next"' in link:
            start = link.find("<") + 1
            end = link.find(">")
            if start > 0 and end > start:
                return link[start:end]

    return None


def fetch_shopify_orders(start_date: str, end_date: str) -> list[dict]:
    creds = get_shopify_creds()
    domain = creds["domain"]
    api_token = creds["api_token"]

    # Shopify accepts ISO timestamps with offset. This matches Mexico store-local reporting.
    start_date_api = f"{start_date}T00:00:00-06:00"
    end_date_api = f"{end_date}T23:59:59-06:00"

    url = (
        f"https://{domain}/admin/api/{SHOPIFY_API_VERSION}/orders.json"
        f"?status=any"
        f"&limit=250"
        f"&created_at_min={start_date_api}"
        f"&created_at_max={end_date_api}"
    )

    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": api_token,
    }

    orders = []

    while url:
        response = requests.get(url, headers=headers, timeout=45)

        if response.status_code != 200:
            raise RuntimeError(
                f"Error fetching Shopify orders: {response.status_code} - {response.text}"
            )

        data = response.json()
        results = data.get("orders", [])
        orders.extend(results)

        url = get_next_page_url(response.headers.get("Link"))

    return orders


KIT_AGUA_DE_ROSAS_PRICE = 175.0
KIT_EXFOLIANTE_PRICE = 264.0


def split_shopify_amount_by_product(products: list[str], amount: float, quantity: int) -> dict[str, float]:
    """
    Splits Shopify line sales by product.

    Business rule for kits:
    - agua_de_rosas gets $175 per kit unit
    - exfoliante gets $264 per kit unit
    - remainder goes to the IPL product

    For non-kits, the full amount goes to the product.
    """

    if not products:
        return {}

    products_set = set(products)

    is_kit = (
        ("ipl_pro" in products_set or "ipl_cl" in products_set)
        and "agua_de_rosas" in products_set
        and "exfoliante" in products_set
    )

    if not is_kit:
        return {product: amount / len(products) for product in products}

    agua_amount = KIT_AGUA_DE_ROSAS_PRICE * quantity
    exfoliante_amount = KIT_EXFOLIANTE_PRICE * quantity
    fixed_kit_amount = agua_amount + exfoliante_amount

    ipl_product = "ipl_pro" if "ipl_pro" in products_set else "ipl_cl"
    ipl_amount = max(amount - fixed_kit_amount, 0.0)

    return {
        ipl_product: ipl_amount,
        "exfoliante": exfoliante_amount,
        "agua_de_rosas": agua_amount,
    }


def shopify_by_day(start_date: str, end_date: str, return_raw: bool = False):
    orders = fetch_shopify_orders(start_date, end_date)
    rows = []

    valid_financial_statuses = {"paid", "partially_refunded"}

    for order in orders:
        financial_status = order.get("financial_status")
        if financial_status not in valid_financial_statuses:
            continue

        order_date_raw = order.get("created_at")
        if not order_date_raw:
            continue

        order_date = pd.to_datetime(order_date_raw).tz_convert(SHOPIFY_TIMEZONE).date()

        for item in order.get("line_items", []):
            raw_title = item.get("name") or item.get("title")
            products = normalize_shopify_title(raw_title)

            if not products:
                continue

            quantity = extract_quantity(item)
            amount = extract_line_amount(item)
            amount_by_product = split_shopify_amount_by_product(products, amount, quantity)

            for product, product_amount in amount_by_product.items():
                rows.append(
                    {
                        "date": order_date,
                        "mkp_name": SHOPIFY_MARKETPLACE_NAME,
                        "financial_status": financial_status,
                        "raw_title": raw_title,
                        "product": product,
                        "quantity": quantity,
                        "amount": product_amount,
                    }
                )

    df_raw = pd.DataFrame(rows)

    if df_raw.empty:
        df_units = pd.DataFrame(columns=DB_COLUMNS)
        df_sales = pd.DataFrame(columns=DB_COLUMNS)
        if return_raw:
            return df_units, df_sales, df_raw
        return df_units, df_sales

    df_units = (
        df_raw.pivot_table(
            index=["date", "mkp_name"],
            columns="product",
            values="quantity",
            aggfunc="sum",
            fill_value=0,
        )
        .reindex(columns=PRODUCT_COLUMNS, fill_value=0)
        .reset_index()
    )

    df_sales = (
        df_raw.pivot_table(
            index=["date", "mkp_name"],
            columns="product",
            values="amount",
            aggfunc="sum",
            fill_value=0,
        )
        .reindex(columns=PRODUCT_COLUMNS, fill_value=0)
        .reset_index()
    )

    df_units.columns.name = None
    df_sales.columns.name = None

    df_units = df_units[DB_COLUMNS].copy()
    df_sales = df_sales[DB_COLUMNS].copy()

    for col in PRODUCT_COLUMNS:
        df_units[col] = pd.to_numeric(df_units[col], errors="coerce").fillna(0).astype(int)
        df_sales[col] = pd.to_numeric(df_sales[col], errors="coerce").fillna(0).round(2)

    if return_raw:
        return df_units, df_sales, df_raw

    return df_units, df_sales


def update_shopify_date_range(start_date: str, end_date: str) -> dict:
    df_units, df_sales = shopify_by_day(start_date, end_date)

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


def update_shopify_last_weeks(weeks: int) -> dict:
    if weeks < 1:
        raise ValueError("Weeks must be at least 1.")

    end_dt = date.today() - timedelta(days=1)
    start_dt = end_dt - timedelta(days=weeks * 7 - 1)

    return update_shopify_date_range(
        start_date=start_dt.isoformat(),
        end_date=end_dt.isoformat(),
    )
