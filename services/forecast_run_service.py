import pandas as pd

from services.forecast_prepare_service import (
    get_sales_series_for_forecast,
    get_units_series_for_forecast,
    get_cashflow_history,
)

try:
    from prophet import Prophet
except ImportError as exc:
    raise ImportError(
        "Prophet is not installed. Install it with: pip install prophet"
    ) from exc


def forecast_series(
    df: pd.DataFrame,
    periods: int = 15,
    freq: str = "D",
    growth: str = "linear",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Forecast a time series using Prophet.

    Expected input columns:
    - ds
    - y

    Returns:
    - history_df: original cleaned history
    - forecast_df: Prophet output for future periods only
    """
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


def run_sales_forecast(periods: int = 15) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
    - sales_history: columns ds, y
    - sales_forecast: columns ds, yhat, yhat_lower, yhat_upper
    """
    sales_df = get_sales_series_for_forecast()
    return forecast_series(sales_df, periods=periods)


def run_units_forecast(periods: int = 15) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
    - units_history: columns ds, y
    - units_forecast: columns ds, yhat, yhat_lower, yhat_upper
    """
    units_df = get_units_series_for_forecast()
    return forecast_series(units_df, periods=periods)



def run_cashflow_projection(
    periods: int = 15,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build future cash flow projection using:
    - forecasted sales
    - already loaded future expenses from DB
    - last known bank balance

    Returns:
    - history_df:
        date, sales_total, expenses_total, banks_total, net_income
    - projection_df:
        ds,
        projected_sales,
        projected_sales_min,
        projected_sales_max,
        projected_expenses,
        projected_net_income,
        projected_net_income_min,
        projected_net_income_max,
        projected_bank_balance,
        projected_bank_balance_min,
        projected_bank_balance_max
    """
    cashflow_history = get_cashflow_history()

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

    sales_series = get_sales_series_for_forecast()
    _, sales_forecast = forecast_series(sales_series, periods=periods)

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

    # Bring future expenses from DB history/projection table
    expenses_future = cashflow_history[["date", "expenses_total"]].copy()
    expenses_future["date"] = pd.to_datetime(expenses_future["date"])

    projection_df = projection_df.merge(
        expenses_future,
        left_on="ds",
        right_on="date",
        how="left"
    )

    projection_df["expenses_total"] = (
        pd.to_numeric(projection_df["expenses_total"], errors="coerce").fillna(0).round(2)
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

    latest_bank_balance = (
        pd.to_numeric(cashflow_history["banks_total"], errors="coerce").fillna(0).iloc[-1]
    )

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