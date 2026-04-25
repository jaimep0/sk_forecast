import pandas as pd

from services.acquisition_expense_service import get_acquisition_expense_daily_totals
from services.sales_service import get_sales_weekly_totals
from services.test_data_service import get_test_last_6_weeks_roas

ROAS_COLUMNS = ["date", "sales_total", "acquisition_expense_total", "roas"]


def get_roas_history() -> pd.DataFrame:
    sales_df = get_sales_weekly_totals()
    expense_df = get_acquisition_expense_daily_totals()

    if sales_df.empty and expense_df.empty:
        return pd.DataFrame(columns=ROAS_COLUMNS)

    df = pd.merge(sales_df, expense_df, on="date", how="outer").sort_values("date").reset_index(drop=True)
    df["sales_total"] = pd.to_numeric(df["sales_total"], errors="coerce").fillna(0)
    df["acquisition_expense_total"] = pd.to_numeric(df["acquisition_expense_total"], errors="coerce").fillna(0)
    df["roas"] = (df["sales_total"] / df["acquisition_expense_total"].replace(0, pd.NA)).fillna(0)
    return df[ROAS_COLUMNS]


def get_last_6_weeks_roas() -> pd.DataFrame:
    df = get_roas_history()
    if df.empty:
        return df

    df = df[(df["sales_total"] > 0) & (df["acquisition_expense_total"] > 0)].copy()
    return df.sort_values("date").tail(6).reset_index(drop=True) if not df.empty else df


def get_last_6_weeks_roas_by_mode(mode: str = "shinny") -> pd.DataFrame:
    return get_test_last_6_weeks_roas() if mode == "test" else get_last_6_weeks_roas()
