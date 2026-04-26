import pandas as pd

from services.forecast_prepare_service import (
    get_cashflow_history,
    get_sales_series_for_forecast,
    get_units_series_for_forecast,
)
from services.test_data_service import (
    get_test_sales_series_for_forecast,
    get_test_units_series_for_forecast,
    get_test_banks_daily_totals,
    get_test_cashflow_history,
)

try:
    from prophet import Prophet
except ImportError as exc:
    raise ImportError("Prophet is not installed. Install it with: pip install prophet") from exc


FREQ_MAP = {
    "daily": "D",
    "weekly": "W-SUN",
    "monthly": "M",
}

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
    """Forecast a ds/y series and keep only recent history plus future periods."""
    empty_history = pd.DataFrame(columns=["ds", "y"])
    empty_forecast = pd.DataFrame(columns=["ds", "yhat", "yhat_lower", "yhat_upper", "real"])

    if df.empty:
        return empty_history, empty_forecast

    history_df = df.copy()[["ds", "y"]]
    history_df["ds"] = pd.to_datetime(history_df["ds"], errors="coerce")
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

    df_final = forecast_df.merge(
        history_df.rename(columns={"y": "real"}),
        on="ds",
        how="left",
    )

    last_history_date = history_df["ds"].max()
    past_part = df_final[df_final["ds"] <= last_history_date].tail(past_periods_to_show)
    future_part = df_final[df_final["ds"] > last_history_date]

    df_final = pd.concat([past_part, future_part], ignore_index=True)
    return history_df, df_final


def _freq_to_prophet_rule(freq: str) -> str:
    return FREQ_MAP.get(freq, "D")


def _series_by_mode(mode: str, freq: str, kind: str) -> pd.DataFrame:
    if kind == "sales":
        if mode == "test":
            return get_test_sales_series_for_forecast(freq=freq)
        return get_sales_series_for_forecast(freq=freq)

    if mode == "test":
        return get_test_units_series_for_forecast(freq=freq)
    return get_units_series_for_forecast(freq=freq)


def _empty_projection() -> pd.DataFrame:
    return pd.DataFrame(columns=[*PROJECTION_COLUMNS, "real_balance"])


def _clean_cashflow_history(cashflow_history: pd.DataFrame) -> pd.DataFrame:
    """Normalize cashflow history columns without inventing fake bank balances."""
    required_columns = ["date", "sales_total", "expenses_total", "banks_total", "net_income"]

    if cashflow_history.empty:
        return pd.DataFrame(columns=required_columns)

    out = cashflow_history.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"])

    for col in ["sales_total", "expenses_total", "banks_total"]:
        if col not in out.columns:
            out[col] = pd.NA

    out["sales_total"] = pd.to_numeric(out["sales_total"], errors="coerce").fillna(0)
    out["expenses_total"] = pd.to_numeric(out["expenses_total"], errors="coerce").fillna(0)

    # Do not fill bank balances with 0. Missing bank balances must stay missing.
    out["banks_total"] = pd.to_numeric(out["banks_total"], errors="coerce")

    out["net_income"] = out["sales_total"] - out["expenses_total"]
    return out[required_columns].sort_values("date").reset_index(drop=True)


def _real_balances_by_mode(
    mode: str,
    freq: str,
    cashflow_history: pd.DataFrame,
) -> pd.DataFrame:
    """
    Return real bank balances aligned to the selected frequency.

    Important:
    - Do NOT forward-fill bank balances.
    - Do NOT create 0 balances for missing dates.
    - Only dates with real loaded bank data should appear as real_balance.
    """
    rule = _freq_to_prophet_rule(freq)

    if mode == "test":
        banks_df = get_test_banks_daily_totals().copy()
    else:
        banks_df = cashflow_history[["date", "banks_total"]].copy()

    if banks_df.empty:
        return pd.DataFrame(columns=["ds", "real_balance"])

    banks_df["date"] = pd.to_datetime(banks_df["date"], errors="coerce")
    banks_df["banks_total"] = pd.to_numeric(banks_df["banks_total"], errors="coerce")
    banks_df = banks_df.dropna(subset=["date", "banks_total"])

    # Very important: remove fake zeros.
    banks_df = banks_df[banks_df["banks_total"] > 0]

    if banks_df.empty:
        return pd.DataFrame(columns=["ds", "real_balance"])

    if freq == "daily":
        real_balances = banks_df.rename(
            columns={
                "date": "ds",
                "banks_total": "real_balance",
            }
        )[["ds", "real_balance"]]

    else:
        # Align real bank dates to the same period labels used by the forecast.
        # Weekly forecast uses W-SUN, so a Monday bank row belongs to the Sunday
        # closing period of that week.
        real_balances = (
            banks_df.set_index("date")[["banks_total"]]
            .resample(rule)
            .last()
            .dropna()
            .reset_index()
            .rename(columns={"date": "ds", "banks_total": "real_balance"})
        )

    real_balances["ds"] = pd.to_datetime(real_balances["ds"], errors="coerce")
    real_balances["real_balance"] = pd.to_numeric(real_balances["real_balance"], errors="coerce")

    real_balances = real_balances.dropna(subset=["ds", "real_balance"])
    real_balances = real_balances[real_balances["real_balance"] > 0]

    return real_balances.sort_values("ds").reset_index(drop=True)


def _patch_history_with_real_balances(
    cashflow_history: pd.DataFrame,
    real_balances: pd.DataFrame,
) -> pd.DataFrame:
    """Replace bad/fake bank totals in history with the clean aligned balances."""
    if cashflow_history.empty:
        return cashflow_history

    out = cashflow_history.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")

    if real_balances.empty:
        out["banks_total"] = pd.NA
        return out

    clean_real = real_balances.rename(columns={"ds": "date", "real_balance": "clean_banks_total"})
    clean_real["date"] = pd.to_datetime(clean_real["date"], errors="coerce")

    out = out.drop(columns=["clean_banks_total"], errors="ignore")
    out = out.merge(clean_real[["date", "clean_banks_total"]], on="date", how="left")
    out["banks_total"] = pd.to_numeric(out["clean_banks_total"], errors="coerce")
    out = out.drop(columns=["clean_banks_total"])
    return out.sort_values("date").reset_index(drop=True)


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
    raw_cashflow_history = get_test_cashflow_history(freq=freq) if mode == "test" else get_cashflow_history(freq=freq)
    cashflow_history = _clean_cashflow_history(raw_cashflow_history)
    sales_series = _series_by_mode(mode, freq, "sales")

    if cashflow_history.empty:
        return cashflow_history, _empty_projection()

    real_balances = _real_balances_by_mode(mode, freq, cashflow_history)
    cashflow_history = _patch_history_with_real_balances(cashflow_history, real_balances)

    _, sales_forecast = forecast_series(
        sales_series,
        periods=periods,
        freq=_freq_to_prophet_rule(freq),
        past_periods_to_show=past_periods_to_show,
    )

    if sales_forecast.empty:
        return cashflow_history, _empty_projection()

    projection = sales_forecast.copy()
    projection["ds"] = pd.to_datetime(projection["ds"], errors="coerce")
    projection = projection.dropna(subset=["ds"])

    expenses = cashflow_history[["date", "expenses_total"]].copy()
    expenses["date"] = pd.to_datetime(expenses["date"], errors="coerce")
    expenses["expenses_total"] = pd.to_numeric(expenses["expenses_total"], errors="coerce").fillna(0)

    projection = projection.merge(
        expenses,
        left_on="ds",
        right_on="date",
        how="left",
    )
    projection = projection.drop(columns=["date"], errors="ignore")
    projection["expenses_total"] = pd.to_numeric(projection["expenses_total"], errors="coerce").fillna(0).round(2)

    projection = projection.merge(real_balances, on="ds", how="left")
    projection["real_balance"] = pd.to_numeric(projection["real_balance"], errors="coerce")

    projection["projected_sales"] = pd.to_numeric(projection["yhat"], errors="coerce").fillna(0).round(2)
    projection["projected_sales_min"] = pd.to_numeric(projection["yhat_lower"], errors="coerce").fillna(0).round(2)
    projection["projected_sales_max"] = pd.to_numeric(projection["yhat_upper"], errors="coerce").fillna(0).round(2)
    projection["projected_expenses"] = projection["expenses_total"]

    projection["projected_net_income"] = (projection["projected_sales"] - projection["projected_expenses"]).round(2)
    projection["projected_net_income_min"] = (projection["projected_sales_min"] - projection["projected_expenses"]).round(2)
    projection["projected_net_income_max"] = (projection["projected_sales_max"] - projection["projected_expenses"]).round(2)

    if real_balances.empty:
        projection["projected_bank_balance"] = pd.NA
        projection["projected_bank_balance_min"] = pd.NA
        projection["projected_bank_balance_max"] = pd.NA
        return cashflow_history, projection[[*PROJECTION_COLUMNS, "real_balance"]].reset_index(drop=True)

    last_real_date = real_balances["ds"].max()
    latest_real_balance = float(
        real_balances.loc[
            real_balances["ds"] == last_real_date,
            "real_balance",
        ].iloc[-1]
    )

    has_real_balance_mask = projection["real_balance"].notna() & (projection["real_balance"] > 0)
    future_mask = projection["ds"] > last_real_date

    # Only rows that truly have bank data should show real/projected historical balance.
    projection.loc[has_real_balance_mask, "projected_bank_balance"] = projection.loc[
        has_real_balance_mask,
        "real_balance",
    ]
    projection.loc[has_real_balance_mask, "projected_bank_balance_min"] = projection.loc[
        has_real_balance_mask,
        "real_balance",
    ]
    projection.loc[has_real_balance_mask, "projected_bank_balance_max"] = projection.loc[
        has_real_balance_mask,
        "real_balance",
    ]

    # Rows without real bank data must stay empty, not 0.
    missing_real_history_mask = (projection["ds"] <= last_real_date) & ~has_real_balance_mask
    projection.loc[missing_real_history_mask, "projected_bank_balance"] = pd.NA
    projection.loc[missing_real_history_mask, "projected_bank_balance_min"] = pd.NA
    projection.loc[missing_real_history_mask, "projected_bank_balance_max"] = pd.NA

    # Future rows: project from the latest actual bank balance.
    future_projection = projection.loc[future_mask].copy()

    if not future_projection.empty:
        projection.loc[future_mask, "projected_bank_balance"] = (
            latest_real_balance + future_projection["projected_net_income"].cumsum()
        ).round(2).values
        projection.loc[future_mask, "projected_bank_balance_min"] = (
            latest_real_balance + future_projection["projected_net_income_min"].cumsum()
        ).round(2).values
        projection.loc[future_mask, "projected_bank_balance_max"] = (
            latest_real_balance + future_projection["projected_net_income_max"].cumsum()
        ).round(2).values

    # Real values must only exist for historical rows. Future real balances should be empty, not 0.
    projection.loc[future_mask, "real_balance"] = pd.NA

    for col in [
        "projected_bank_balance",
        "projected_bank_balance_min",
        "projected_bank_balance_max",
    ]:
        projection[col] = pd.to_numeric(projection[col], errors="coerce").round(2)

    projection["real_balance"] = pd.to_numeric(projection["real_balance"], errors="coerce")
    projection.loc[projection["real_balance"] <= 0, "real_balance"] = pd.NA

    output_columns = [*PROJECTION_COLUMNS, "real_balance"]
    return cashflow_history, projection[output_columns].reset_index(drop=True)
