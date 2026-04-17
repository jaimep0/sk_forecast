import pandas as pd

from services.forecast_prepare_service import (
    get_sales_series_for_forecast,
    get_units_series_for_forecast,
    get_cashflow_history,
)

from services.expenses_service import get_expenses_daily_totals
from services.banks_service import get_banks_daily_totals
from services.test_data_service import get_test_banks_daily_totals

try:
    from prophet import Prophet
except ImportError as exc:
    raise ImportError(
        "Prophet is not installed. Install it with: pip install prophet"
    ) from exc

from services.test_data_service import (
    get_test_sales_series_for_forecast,
    get_test_units_series_for_forecast,
    get_test_cashflow_history,
)


def forecast_series(
    df: pd.DataFrame,
    periods: int = 15,
    freq: str = "D",
    growth: str = "linear",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return (
            pd.DataFrame(columns=["ds", "y"]),
            pd.DataFrame(columns=["ds", "yhat", "yhat_lower", "yhat_upper"]),
        )

    history_df = df.copy()
    history_df = history_df[["ds", "y"]].copy()
    history_df["ds"] = pd.to_datetime(history_df["ds"])
    history_df["y"] = pd.to_numeric(history_df["y"], errors="coerce")
    history_df = history_df.dropna(subset=["ds", "y"]).sort_values("ds").reset_index(drop=True)

    if history_df.empty:
        return (
            pd.DataFrame(columns=["ds", "y"]),
            pd.DataFrame(columns=["ds", "yhat", "yhat_lower", "yhat_upper"]),
        )

    model = Prophet(
        growth=growth,
        weekly_seasonality=True,
        yearly_seasonality=True,
        daily_seasonality=False,
    )
    model.fit(history_df)

    future = model.make_future_dataframe(periods=periods, freq=freq)
    forecast = model.predict(future)

    forecast_df = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    forecast_df[["yhat", "yhat_lower", "yhat_upper"]] = (
        forecast_df[["yhat", "yhat_lower", "yhat_upper"]]
        .clip(lower=0)
        .round(2)
    )

    last_history_date = history_df["ds"].max()
    forecast_df = forecast_df[forecast_df["ds"] > last_history_date].reset_index(drop=True)

    return history_df, forecast_df


def _freq_to_prophet_rule(freq: str) -> str:
    return {
        "daily": "D",
        "weekly": "W-SUN",
        "monthly": "M",
    }[freq]


def run_sales_forecast(
    periods: int = 15,
    freq: str = "daily",
    mode: str = "shinny",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if mode == "test":
        sales_df = get_test_sales_series_for_forecast(freq=freq)
    else:
        sales_df = get_sales_series_for_forecast(freq=freq)

    return forecast_series(sales_df, periods=periods, freq=_freq_to_prophet_rule(freq))



def run_units_forecast(
    periods: int = 15,
    freq: str = "daily",
    mode: str = "shinny",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if mode == "test":
        units_df = get_test_units_series_for_forecast(freq=freq)
    else:
        units_df = get_units_series_for_forecast(freq=freq)

    return forecast_series(units_df, periods=periods, freq=_freq_to_prophet_rule(freq))


def run_cashflow_projection(
    periods: int = 15,
    freq: str = "daily",
    mode: str = "shinny",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if mode == "test":
        cashflow_history = get_test_cashflow_history(freq=freq)
        sales_series = get_test_sales_series_for_forecast(freq=freq)
    else:
        cashflow_history = get_cashflow_history(freq=freq)
        sales_series = get_sales_series_for_forecast(freq=freq)

    if cashflow_history.empty:
        empty_history = pd.DataFrame(
            columns=["date", "sales_total", "expenses_total", "banks_total", "net_income"]
        )
        empty_projection = pd.DataFrame(
            columns=[
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
        )
        return empty_history, empty_projection

    _, sales_forecast = forecast_series(
        sales_series,
        periods=periods,
        freq=_freq_to_prophet_rule(freq),
    )

    if sales_forecast.empty:
        empty_projection = pd.DataFrame(
            columns=[
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
        )
        return cashflow_history, empty_projection

    projection_df = sales_forecast.copy()
    projection_df["ds"] = pd.to_datetime(projection_df["ds"])

    expenses_future = cashflow_history[["date", "expenses_total"]].copy()
    expenses_future["date"] = pd.to_datetime(expenses_future["date"])

    projection_df = projection_df.merge(
        expenses_future,
        left_on="ds",
        right_on="date",
        how="left"
    )

    projection_df["expenses_total"] = (
        pd.to_numeric(projection_df["expenses_total"], errors="coerce")
        .fillna(0)
        .round(2)
    )

    projection_df["projected_sales"] = projection_df["yhat"].round(2)
    projection_df["projected_sales_min"] = projection_df["yhat_lower"].round(2)
    projection_df["projected_sales_max"] = projection_df["yhat_upper"].round(2)

    projection_df["projected_expenses"] = projection_df["expenses_total"]

    projection_df["projected_net_income"] = (
        projection_df["projected_sales"] - projection_df["projected_expenses"]
    ).round(2)

    projection_df["projected_net_income_min"] = (
        projection_df["projected_sales_min"] - projection_df["projected_expenses"]
    ).round(2)

    projection_df["projected_net_income_max"] = (
        projection_df["projected_sales_max"] - projection_df["projected_expenses"]
    ).round(2)

    if mode == "test":
        banks_df = get_test_banks_daily_totals()
        latest_bank_balance = float(banks_df["banks_total"].iloc[-1]) if not banks_df.empty else 0
    else:
        latest_bank_balance = float(cashflow_history["banks_total"].iloc[-1]) if not cashflow_history.empty else 0

    projection_df["projected_bank_balance"] = (
        latest_bank_balance + projection_df["projected_net_income"].cumsum()
    ).round(2)

    projection_df["projected_bank_balance_min"] = (
        latest_bank_balance + projection_df["projected_net_income_min"].cumsum()
    ).round(2)

    projection_df["projected_bank_balance_max"] = (
        latest_bank_balance + projection_df["projected_net_income_max"].cumsum()
    ).round(2)

    projection_df = projection_df[
        [
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
    ].reset_index(drop=True)

    return cashflow_history, projection_df
