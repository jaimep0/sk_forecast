import pandas as pd

from services.forecast_service import (
    get_sales_forecast_base_data,
    get_cashflow_base_data,
)


def resample_series(df: pd.DataFrame, value_col: str, freq: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["ds", "y"])

    freq_map = {
        "daily": "D",
        "weekly": "W-SUN",
        "monthly": "M",
    }

    rule = freq_map[freq]

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])

    out = (
        out.set_index("date")[[value_col]]
        .resample(rule)
        .sum()
        .reset_index()
        .rename(columns={"date": "ds", value_col: "y"})
    )

    out["y"] = pd.to_numeric(out["y"], errors="coerce").fillna(0)
    return out.sort_values("ds").reset_index(drop=True)


def get_sales_series_for_forecast(freq: str = "daily") -> pd.DataFrame:
    sales_df, _ = get_sales_forecast_base_data()

    if sales_df.empty:
        return pd.DataFrame(columns=["ds", "y"])

    return resample_series(sales_df, "sales_total", freq)


def get_units_series_for_forecast(freq: str = "daily") -> pd.DataFrame:
    _, units_df = get_sales_forecast_base_data()

    if units_df.empty:
        return pd.DataFrame(columns=["ds", "y"])

    return resample_series(units_df, "units_total", freq)


def get_cashflow_history(freq: str = "daily") -> pd.DataFrame:
    df = get_cashflow_base_data()

    if df.empty:
        return pd.DataFrame(
            columns=["date", "sales_total", "expenses_total", "banks_total", "net_income"]
        )

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    for col in ["sales_total", "expenses_total", "banks_total"]:
        if col not in df.columns:
            df[col] = 0

    df["sales_total"] = pd.to_numeric(df["sales_total"], errors="coerce").fillna(0)
    df["expenses_total"] = pd.to_numeric(df["expenses_total"], errors="coerce").fillna(0)
    df["banks_total"] = pd.to_numeric(df["banks_total"], errors="coerce").fillna(0)

    freq_map = {
        "daily": "D",
        "weekly": "W-SUN",
        "monthly": "M",
    }

    rule = freq_map[freq]

    grouped = (
        df.set_index("date")[["sales_total", "expenses_total"]]
        .resample(rule)
        .sum()
        .reset_index()
    )

    banks = (
        df.set_index("date")[["banks_total"]]
        .resample(rule)
        .last()
        .reset_index()
    )

    out = grouped.merge(banks, on="date", how="left")
    out["banks_total"] = out["banks_total"].ffill().fillna(0)
    out["net_income"] = out["sales_total"] - out["expenses_total"]

    return out.sort_values("date").reset_index(drop=True)