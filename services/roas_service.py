import pandas as pd

from services.sales_service import get_sales_weekly_totals
from services.acquisition_expense_service import get_acquisition_expense_daily_totals
from services.test_data_service import get_test_last_6_weeks_roas


def get_roas_history() -> pd.DataFrame:
    sales_df = get_sales_weekly_totals()
    expense_df = get_acquisition_expense_daily_totals()

    if sales_df.empty and expense_df.empty:
        return pd.DataFrame(columns=["date", "sales_total", "acquisition_expense_total", "roas"])

    df = pd.merge(sales_df, expense_df, on="date", how="outer")
    df = df.sort_values("date").reset_index(drop=True)

    df["sales_total"] = pd.to_numeric(df["sales_total"], errors="coerce").fillna(0)
    df["acquisition_expense_total"] = pd.to_numeric(
        df["acquisition_expense_total"], errors="coerce"
    ).fillna(0)

    df["roas"] = df.apply(
        lambda row: row["sales_total"] / row["acquisition_expense_total"]
        if row["acquisition_expense_total"] > 0
        else 0,
        axis=1,
    )

    return df


def get_last_6_weeks_roas() -> pd.DataFrame:
    df = get_roas_history()

    if df.empty:
        return df

    df = df.copy()
    df["sales_total"] = pd.to_numeric(df["sales_total"], errors="coerce").fillna(0)
    df["acquisition_expense_total"] = pd.to_numeric(
        df["acquisition_expense_total"], errors="coerce"
    ).fillna(0)

    # Keep only weeks with complete data on both sides
    df = df[
        (df["sales_total"] > 0) &
        (df["acquisition_expense_total"] > 0)
    ].copy()

    if df.empty:
        return df

    return df.sort_values("date").tail(6).reset_index(drop=True)

def get_last_6_weeks_roas_by_mode(mode: str = "shinny") -> pd.DataFrame:
    if mode == "test":
        return get_test_last_6_weeks_roas()
    return get_last_6_weeks_roas()