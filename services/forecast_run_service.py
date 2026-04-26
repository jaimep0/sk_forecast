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
    past_periods_to_show: int = 4,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    empty_history = pd.DataFrame(columns=["ds", "y"])
    empty_forecast = pd.DataFrame(columns=["ds", "yhat", "yhat_lower", "yhat_upper", "real"])

    if df.empty:
        return empty_history, empty_forecast

    history_df = df.copy()[["ds", "y"]]
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

    future = model.make_future_dataframe(periods=periods, freq=freq)
    forecast = model.predict(future)

    forecast_df = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    forecast_df[["yhat", "yhat_lower", "yhat_upper"]] = (
        forecast_df[["yhat", "yhat_lower", "yhat_upper"]]
        .clip(lower=0)
        .round(2)
    )

    # merge real values to be able to plot dots on historical part
    df_final = forecast_df.merge(
        history_df.rename(columns={"y": "real"}),
        on="ds",
        how="left",
    )

    # keep only last N historical fitted rows + all future rows
    last_history_date = history_df["ds"].max()
    past_part = df_final[df_final["ds"] <= last_history_date].tail(past_periods_to_show)
    future_part = df_final[df_final["ds"] > last_history_date]

    df_final = pd.concat([past_part, future_part], ignore_index=True)

    return history_df, df_final


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
    past_periods_to_show: int = 4,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return forecast_series(
        _series_by_mode(mode, freq, "sales"),
        periods=periods,
        freq=_freq_to_prophet_rule(freq),
        past_periods_to_show=past_periods_to_show,
    )


def run_units_forecast(
    periods: int = 15,
    freq: str = "daily",
    mode: str = "shinny",
    past_periods_to_show: int = 4,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return forecast_series(
        _series_by_mode(mode, freq, "units"),
        periods=periods,
        freq=_freq_to_prophet_rule(freq),
        past_periods_to_show=past_periods_to_show,
    )


def run_cashflow_projection(
    periods: int = 15,
    freq: str = "daily",
    mode: str = "shinny",
    past_periods_to_show: int = 4,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cashflow_history = get_test_cashflow_history(freq=freq) if mode == "test" else get_cashflow_history(freq=freq)
    sales_series = _series_by_mode(mode, freq, "sales")

    empty_projection = pd.DataFrame(columns=[*PROJECTION_COLUMNS, "real_balance"])
    if cashflow_history.empty:
        empty_history = pd.DataFrame(columns=["date", "sales_total", "expenses_total", "banks_total", "net_income"])
        return empty_history, empty_projection

    _, sales_forecast = forecast_series(
        sales_series,
        periods=periods,
        freq=_freq_to_prophet_rule(freq),
        past_periods_to_show=past_periods_to_show,
    )
    if sales_forecast.empty:
        return cashflow_history, empty_projection

    projection_df = sales_forecast.copy()
    projection_df["ds"] = pd.to_datetime(projection_df["ds"])

    expenses_future = cashflow_history[["date", "expenses_total"]].copy()
    expenses_future["date"] = pd.to_datetime(expenses_future["date"])
    projection_df = projection_df.merge(expenses_future, left_on="ds", right_on="date", how="left")
    projection_df["expenses_total"] = pd.to_numeric(projection_df["expenses_total"], errors="coerce").fillna(0).round(2)

    real_balances = cashflow_history[["date", "banks_total"]].copy()
    real_balances["date"] = pd.to_datetime(real_balances["date"])
    projection_df = projection_df.merge(real_balances, left_on="ds", right_on="date", how="left", suffixes=("", "_real"))
    projection_df["real_balance"] = pd.to_numeric(projection_df["banks_total"], errors="coerce")

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

    output_columns = [*PROJECTION_COLUMNS, "real_balance"]
    return cashflow_history, projection_df[output_columns].reset_index(drop=True)

    freq_code = FREQ_MAP[freq]

    if mode == "test":
        from services.test_data_service import (
            get_test_sales_series_for_forecast,
            get_test_expenses_daily_totals,
            get_test_banks_daily_totals,
        )

        sales_df = get_test_sales_series_for_forecast(freq="daily")
        expenses_df = get_test_expenses_daily_totals().copy()
        banks_df = get_test_banks_daily_totals().copy()
    else:
        sales_df = get_sales_series_for_forecast(freq="daily")
        expenses_df = get_expenses_daily_totals().copy()
        banks_df = get_banks_daily_totals().copy()

    empty_projection = pd.DataFrame(columns=PROJECTION_COLUMNS + ["real_balance"])

    if sales_df.empty:
        return pd.DataFrame(columns=["ds", "y"]), empty_projection

    # ---- sales forecast (daily) ----
    sales_history, sales_forecast = forecast_series(
        sales_df,
        periods=periods * 31 if freq != "daily" else periods,
        freq="D",
        past_periods_to_show=4,
    )

    if sales_forecast.empty:
        return sales_history, empty_projection

    sales_forecast = sales_forecast.rename(
        columns={
            "yhat": "projected_sales",
            "yhat_lower": "projected_sales_min",
            "yhat_upper": "projected_sales_max",
            "real": "real_sales",
        }
    )

    sales_forecast["ds"] = pd.to_datetime(sales_forecast["ds"])

    # ---- expenses ----
    expenses_df["date"] = pd.to_datetime(expenses_df["date"])
    expenses_df["expenses_total"] = pd.to_numeric(expenses_df["expenses_total"], errors="coerce").fillna(0)

    # ---- banks ----
    banks_df["date"] = pd.to_datetime(banks_df["date"])
    banks_df["banks_total"] = pd.to_numeric(banks_df["banks_total"], errors="coerce").fillna(0)

    latest_real_balance = float(banks_df.sort_values("date")["banks_total"].iloc[-1]) if not banks_df.empty else 0.0
    last_real_balance_date = banks_df["date"].max() if not banks_df.empty else None

    # ---- merge expenses into forecast ----
    projection = sales_forecast.merge(
        expenses_df[["date", "expenses_total"]],
        left_on="ds",
        right_on="date",
        how="left",
    )

    projection["expenses_total"] = projection["expenses_total"].fillna(0)
    projection = projection.drop(columns=["date"])

    # ---- net income ----
    projection["projected_net_income"] = projection["projected_sales"] - projection["expenses_total"]
    projection["projected_net_income_min"] = projection["projected_sales_min"] - projection["expenses_total"]
    projection["projected_net_income_max"] = projection["projected_sales_max"] - projection["expenses_total"]

    # ---- split past fitted rows vs future rows ----
    if last_real_balance_date is not None:
        past_mask = projection["ds"] <= last_real_balance_date
        future_mask = projection["ds"] > last_real_balance_date
    else:
        past_mask = pd.Series([False] * len(projection), index=projection.index)
        future_mask = ~past_mask

    # real balances for past rows
    projection = projection.merge(
        banks_df[["date", "banks_total"]],
        left_on="ds",
        right_on="date",
        how="left",
    )
    projection["real_balance"] = projection["banks_total"]
    projection = projection.drop(columns=["date", "banks_total"])

    # future projected balances start from latest real balance
    future_projection = projection[future_mask].copy()

    if not future_projection.empty:
        future_projection["projected_bank_balance"] = latest_real_balance + future_projection["projected_net_income"].cumsum()
        future_projection["projected_bank_balance_min"] = latest_real_balance + future_projection["projected_net_income_min"].cumsum()
        future_projection["projected_bank_balance_max"] = latest_real_balance + future_projection["projected_net_income_max"].cumsum()

    past_projection = projection[past_mask].copy()

    # for past fitted section, use real balance as the displayed projected line baseline
    if not past_projection.empty:
        past_projection["projected_bank_balance"] = past_projection["real_balance"]
        past_projection["projected_bank_balance_min"] = past_projection["real_balance"]
        past_projection["projected_bank_balance_max"] = past_projection["real_balance"]

    # keep only last 4 historical rows
    past_projection = past_projection.tail(4)

    combined = pd.concat([past_projection, future_projection], ignore_index=True)

    # ---- optional regrouping after daily logic ----
    if freq != "daily" and not combined.empty:
        combined = combined.copy()
        combined["ds"] = pd.to_datetime(combined["ds"])

        agg_map = {
            "projected_sales": "sum",
            "projected_sales_min": "sum",
            "projected_sales_max": "sum",
            "expenses_total": "sum",
            "projected_net_income": "sum",
            "projected_net_income_min": "sum",
            "projected_net_income_max": "sum",
            "projected_bank_balance": "last",
            "projected_bank_balance_min": "last",
            "projected_bank_balance_max": "last",
            "real_balance": "last",
        }

        combined = (
            combined.set_index("ds")
            .resample(freq_code)
            .agg(agg_map)
            .reset_index()
        )

    return sales_history, combined