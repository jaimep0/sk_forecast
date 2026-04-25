import pandas as pd

from services.banks_service import get_banks_daily_totals
from services.expenses_service import get_expenses_daily_totals
from services.sales_service import get_sales_daily_totals
from services.units_service import get_units_daily_totals


def get_sales_forecast_base_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    return get_sales_daily_totals(), get_units_daily_totals()


def get_cashflow_base_data() -> pd.DataFrame:
    df = pd.merge(get_sales_daily_totals(), get_expenses_daily_totals(), on="date", how="outer")
    df = pd.merge(df, get_banks_daily_totals(), on="date", how="outer")
    return df.fillna(0).sort_values("date").reset_index(drop=True)
