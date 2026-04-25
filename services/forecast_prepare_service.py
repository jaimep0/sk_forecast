import pandas as pd

from services.forecast_service import get_cashflow_base_data, get_sales_forecast_base_data

FREQ_MAP = {"daily": "D", "weekly": "W-SUN", "monthly": "M"}


def resample_series(df: pd.DataFrame, value_col: str, freq: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["ds", "y"])

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    out = (
        out.set_index("date")[[value_col]]
        .resample(FREQ_MAP[freq]).sum()
        .reset_index()
        .rename(columns={"date": "ds", value_col: "y"})
    )
    out["y"] = pd.to_numeric(out["y"], errors="coerce").fillna(0)
    return out.sort_values("ds").reset_index(drop=True)


def get_sales_series_for_forecast(freq: str = "daily") -> pd.DataFrame:
    sales_df, _ = get_sales_forecast_base_data()
    return pd.DataFrame(columns=["ds", "y"]) if sales_df.empty else resample_series(sales_df, "sales_total", freq)


def get_units_series_for_forecast(freq: str = "daily") -> pd.DataFrame:
    _, units_df = get_sales_forecast_base_data()
    return pd.DataFrame(columns=["ds", "y"]) if units_df.empty else resample_series(units_df, "units_total", freq)


def get_cashflow_history(freq: str = "daily") -> pd.DataFrame:
    df = get_cashflow_base_data()
    columns = ["date", "sales_total", "expenses_total", "banks_total", "net_income"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    for col in ["sales_total", "expenses_total", "banks_total"]:
        df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0)

    grouped = (
        df.set_index("date")[["sales_total", "expenses_total"]]
        .resample(FREQ_MAP[freq]).sum()
        .reset_index()
    )
    banks = df.set_index("date")[["banks_total"]].resample(FREQ_MAP[freq]).last().reset_index()

    out = grouped.merge(banks, on="date", how="left")
    out["banks_total"] = out["banks_total"].ffill().fillna(0)
    out["net_income"] = out["sales_total"] - out["expenses_total"]
    return out[columns].sort_values("date").reset_index(drop=True)
