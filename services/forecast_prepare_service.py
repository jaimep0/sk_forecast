import pandas as pd

from services.forecast_service import (
    get_sales_forecast_base_data,
    get_cashflow_base_data,
)


def get_sales_series_for_forecast() -> pd.DataFrame:
    sales_df, _ = get_sales_forecast_base_data()

    if sales_df.empty:
        return pd.DataFrame(columns=["ds", "y"])

    df = sales_df.copy()
    df = df.rename(columns={"date": "ds", "sales_total": "y"})
    df["ds"] = pd.to_datetime(df["ds"])
    df["y"] = pd.to_numeric(df["y"], errors="coerce").fillna(0)

    return df.sort_values("ds").reset_index(drop=True)


def get_units_series_for_forecast() -> pd.DataFrame:
    _, units_df = get_sales_forecast_base_data()

    if units_df.empty:
        return pd.DataFrame(columns=["ds", "y"])

    df = units_df.copy()
    df = df.rename(columns={"date": "ds", "units_total": "y"})
    df["ds"] = pd.to_datetime(df["ds"])
    df["y"] = pd.to_numeric(df["y"], errors="coerce").fillna(0)

    return df.sort_values("ds").reset_index(drop=True)


def get_cashflow_history() -> pd.DataFrame:
    df = get_cashflow_base_data()

    if df.empty:
        return pd.DataFrame(
            columns=["date", "sales_total", "expenses_total", "banks_total", "net_income"]
        )

    df = df.copy()

    for col in ["sales_total", "expenses_total", "banks_total"]:
        if col not in df.columns:
            df[col] = 0

    df["sales_total"] = pd.to_numeric(df["sales_total"], errors="coerce").fillna(0)
    df["expenses_total"] = pd.to_numeric(df["expenses_total"], errors="coerce").fillna(0)
    df["banks_total"] = pd.to_numeric(df["banks_total"], errors="coerce").fillna(0)

    df["net_income"] = df["sales_total"] - df["expenses_total"]

    return df.sort_values("date").reset_index(drop=True)