import pandas as pd

from services.sales_service import get_sales_daily_totals
from services.units_service import get_units_daily_totals
from services.expenses_service import get_expenses_daily_totals
from services.banks_service import get_banks_daily_totals


def get_sales_forecast_base_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    sales_df = get_sales_daily_totals()
    units_df = get_units_daily_totals()

    return sales_df, units_df


def get_cashflow_base_data() -> pd.DataFrame:
    sales_df = get_sales_daily_totals()
    expenses_df = get_expenses_daily_totals()
    banks_df = get_banks_daily_totals()

    df = pd.merge(sales_df, expenses_df, on="date", how="outer")
    df = pd.merge(df, banks_df, on="date", how="outer")

    df = df.fillna(0).sort_values("date").reset_index(drop=True)

    return df