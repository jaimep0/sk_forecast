import pandas as pd

from services.forecast_prepare_service import (
    get_cashflow_history,
    get_sales_series_for_forecast,
    get_units_series_for_forecast,
)
from services.test_data_service import (
    get_test_banks_daily_totals,
    get_test_cashflow_history,
    get_test_sales_series_for_forecast,
    get_test_units_series_for_forecast,
)

try:
    from prophet import Prophet
except ImportError as exc:
    raise ImportError("Prophet is not installed. Install it with: pip install prophet") from exc

FREQ_MAP = {"daily": "D", "weekly": "W-SUN", "monthly": "M"}
PROJECTION_COLUMNS = [
    "ds",
    "projected_sales",
    "projected_sales_min",
    "projected_sales_max",
    "projected_expenses",
    "projected_net_income",
    "projected_net_income_min",
    "projected_net_income_max",
    "projected_bank_balance",
    "projected_bank_balance_min",
    "projected_bank_balance_max",
]


def forecast_series(
    df: pd.DataFrame,
    periods: int = 15,
    freq: str = "D",
    growth: str = "linear",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    empty_history = pd.DataFrame(columns=["ds", "y"])
    empty_forecast = pd.DataFrame(columns=["ds", "yhat", "yhat_lower", "yhat_upper"])

    if df.empty:
        return empty_history, empty_forecast

    history_df = df[["ds", "y"]].copy()
    history_df["ds"] = pd.to_datetime(history_df["ds"])
    history_df["y"] = pd.to_numeric(history_df["y"], errors="coerce")
    history_df = history_df.dropna(subset=["ds", "y"]).sort_values("ds").reset_index(drop=True)

    if history_df.empty:
        return empty_history, empty_forecast

    model = Prophet(
        growth=growth,
        weekly_seasonality=True,
        yearly_seasonality=True,
        daily_seasonality=False,
    )
    model.fit(history_df)

    forecast_df = model.predict(model.make_future_dataframe(periods=periods, freq=freq))[
        ["ds", "yhat", "yhat_lower", "yhat_upper"]
    ].copy()
    forecast_df[["yhat", "yhat_lower", "yhat_upper"]] = (
        forecast_df[["yhat", "yhat_lower", "yhat_upper"]].clip(lower=0).round(2)
    )
    forecast_df = forecast_df[forecast_df["ds"] > history_df["ds"].max()].reset_index(drop=True)
    return history_df, forecast_df


def _freq_to_prophet_rule(freq: str) -> str:
    return FREQ_MAP[freq]


def _series_by_mode(mode: str, freq: str, kind: str) -> pd.DataFrame:
    if kind == "sales":
        return get_test_sales_series_for_forecast(freq=freq) if mode == "test" else get_sales_series_for_forecast(freq=freq)
    return get_test_units_series_for_forecast(freq=freq) if mode == "test" else get_units_series_for_forecast(freq=freq)


def run_sales_forecast(
    periods: int = 15,
    freq: str = "daily",
    mode: str = "shinny",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return forecast_series(_series_by_mode(mode, freq, "sales"), periods=periods, freq=_freq_to_prophet_rule(freq))


def run_units_forecast(
    periods: int = 15,
    freq: str = "daily",
    mode: str = "shinny",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return forecast_series(_series_by_mode(mode, freq, "units"), periods=periods, freq=_freq_to_prophet_rule(freq))


def run_cashflow_projection(
    periods: int = 15,
    freq: str = "daily",
    mode: str = "shinny",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cashflow_history = get_test_cashflow_history(freq=freq) if mode == "test" else get_cashflow_history(freq=freq)
    sales_series = _series_by_mode(mode, freq, "sales")

    empty_projection = pd.DataFrame(columns=PROJECTION_COLUMNS)
    if cashflow_history.empty:
        empty_history = pd.DataFrame(columns=["date", "sales_total", "expenses_total", "banks_total", "net_income"])
        return empty_history, empty_projection

    _, sales_forecast = forecast_series(sales_series, periods=periods, freq=_freq_to_prophet_rule(freq))
    if sales_forecast.empty:
        return cashflow_history, empty_projection

    projection_df = sales_forecast.copy()
    projection_df["ds"] = pd.to_datetime(projection_df["ds"])

    expenses_future = cashflow_history[["date", "expenses_total"]].copy()
    expenses_future["date"] = pd.to_datetime(expenses_future["date"])
    projection_df = projection_df.merge(expenses_future, left_on="ds", right_on="date", how="left")
    projection_df["expenses_total"] = pd.to_numeric(projection_df["expenses_total"], errors="coerce").fillna(0).round(2)

    projection_df["projected_sales"] = projection_df["yhat"].round(2)
    projection_df["projected_sales_min"] = projection_df["yhat_lower"].round(2)
    projection_df["projected_sales_max"] = projection_df["yhat_upper"].round(2)
    projection_df["projected_expenses"] = projection_df["expenses_total"]

    for suffix, sales_col in [("", "projected_sales"), ("_min", "projected_sales_min"), ("_max", "projected_sales_max")]:
        projection_df[f"projected_net_income{suffix}"] = (projection_df[sales_col] - projection_df["projected_expenses"]).round(2)

    if mode == "test":
        banks_df = get_test_banks_daily_totals()
        latest_bank_balance = float(banks_df["banks_total"].iloc[-1]) if not banks_df.empty else 0
    else:
        latest_bank_balance = float(cashflow_history["banks_total"].iloc[-1]) if not cashflow_history.empty else 0

    for suffix, net_col in [("", "projected_net_income"), ("_min", "projected_net_income_min"), ("_max", "projected_net_income_max")]:
        projection_df[f"projected_bank_balance{suffix}"] = (latest_bank_balance + projection_df[net_col].cumsum()).round(2)

    return cashflow_history, projection_df[PROJECTION_COLUMNS].reset_index(drop=True)
