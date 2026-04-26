import pandas as pd
from pathlib import Path


SAMPLE_DIR = Path("sample_data")


def _read_csv(name: str) -> pd.DataFrame:
    path = SAMPLE_DIR / name
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _ensure_date(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out[col] = pd.to_datetime(out[col])
    return out


def _resample_series(df: pd.DataFrame, value_col: str, freq: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["ds", "y"])

    rule_map = {
        "daily": "D",
        "weekly": "W-SUN",
        "monthly": "M",
    }
    rule = rule_map[freq]

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


def get_test_sales_daily_totals() -> pd.DataFrame:
    df = _read_csv("example_sales.csv")
    if df.empty:
        return pd.DataFrame(columns=["date", "sales_total"])

    df = _ensure_date(df)
    product_cols = [c for c in df.columns if c not in ["date", "mkp_name"]]
    df[product_cols] = df[product_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    df["sales_total"] = df[product_cols].sum(axis=1)

    return df[["date", "sales_total"]].sort_values("date").reset_index(drop=True)


def get_test_units_daily_totals() -> pd.DataFrame:
    df = _read_csv("example_units.csv")
    if df.empty:
        return pd.DataFrame(columns=["date", "units_total"])

    df = _ensure_date(df)
    product_cols = [c for c in df.columns if c not in ["date", "mkp_name"]]
    df[product_cols] = df[product_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    df["units_total"] = df[product_cols].sum(axis=1)

    return df[["date", "units_total"]].sort_values("date").reset_index(drop=True)


def get_test_expenses_daily_totals() -> pd.DataFrame:
    df = _read_csv("example_expenses.csv")
    if df.empty:
        return pd.DataFrame(columns=["date", "expenses_total"])

    df = _ensure_date(df)

    if "total" in df.columns:
        df["total"] = pd.to_numeric(df["total"], errors="coerce").fillna(0)
        out = (
            df.groupby("date", as_index=False)["total"]
            .sum()
            .rename(columns={"total": "expenses_total"})
        )
        return out.sort_values("date").reset_index(drop=True)

    if "expenses_total" in df.columns:
        df["expenses_total"] = pd.to_numeric(df["expenses_total"], errors="coerce").fillna(0)
        return df[["date", "expenses_total"]].sort_values("date").reset_index(drop=True)

    return pd.DataFrame(columns=["date", "expenses_total"])


def get_test_banks_daily_totals() -> pd.DataFrame:
    df = _read_csv("example_banks.csv")
    if df.empty:
        return pd.DataFrame(columns=["date", "banks_total"])

    df = _ensure_date(df)
    bank_cols = [c for c in df.columns if c != "date"]
    df[bank_cols] = df[bank_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    df["banks_total"] = df[bank_cols].sum(axis=1)

    return df[["date", "banks_total"]].sort_values("date").reset_index(drop=True)


def get_test_acquisition_expense_daily_totals() -> pd.DataFrame:
    df = _read_csv("example_acquisition_expense.csv")
    if df.empty:
        return pd.DataFrame(columns=["date", "acquisition_expense_total"])

    df = _ensure_date(df)
    cols = [c for c in df.columns if c != "date"]
    df[cols] = df[cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    df["acquisition_expense_total"] = df[cols].sum(axis=1)

    return df[["date", "acquisition_expense_total"]].sort_values("date").reset_index(drop=True)


def get_test_sales_series_for_forecast(freq: str = "daily") -> pd.DataFrame:
    return _resample_series(get_test_sales_daily_totals(), "sales_total", freq)


def get_test_units_series_for_forecast(freq: str = "daily") -> pd.DataFrame:
    return _resample_series(get_test_units_daily_totals(), "units_total", freq)


def get_test_cashflow_history(freq: str = "daily") -> pd.DataFrame:
    sales_df = get_test_sales_daily_totals()
    expenses_df = get_test_expenses_daily_totals()
    banks_df = get_test_banks_daily_totals()

    df = pd.merge(sales_df, expenses_df, on="date", how="outer")
    df = pd.merge(df, banks_df, on="date", how="outer")

    if df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "sales_total",
                "expenses_total",
                "banks_total",
                "net_income",
            ]
        )

    df["date"] = pd.to_datetime(df["date"])

    for col in ["sales_total", "expenses_total", "banks_total"]:
        if col not in df.columns:
            df[col] = pd.NA

    df["sales_total"] = pd.to_numeric(df["sales_total"], errors="coerce").fillna(0)
    df["expenses_total"] = pd.to_numeric(df["expenses_total"], errors="coerce").fillna(0)
    df["banks_total"] = pd.to_numeric(df["banks_total"], errors="coerce")

    rule_map = {
        "daily": "D",
        "weekly": "W-SUN",
        "monthly": "M",
    }

    rule = rule_map[freq]

    flow = (
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

    out = flow.merge(banks, on="date", how="left")

    # Important: ffill real bank balances, but do not invent zeros.
    out["banks_total"] = pd.to_numeric(out["banks_total"], errors="coerce").ffill()

    out["net_income"] = out["sales_total"] - out["expenses_total"]

    return out.sort_values("date").reset_index(drop=True)


def get_test_roas_history() -> pd.DataFrame:
    sales_df = get_test_sales_daily_totals()
    acq_df = get_test_acquisition_expense_daily_totals()

    if sales_df.empty and acq_df.empty:
        return pd.DataFrame(columns=["date", "sales_total", "acquisition_expense_total", "roas"])

    sales_df["date"] = pd.to_datetime(sales_df["date"])
    acq_df["date"] = pd.to_datetime(acq_df["date"])

    sales_weekly = (
        sales_df.set_index("date")[["sales_total"]]
        .resample("W-SUN")
        .sum()
        .reset_index()
    )

    acq_weekly = (
        acq_df.set_index("date")[["acquisition_expense_total"]]
        .resample("W-SUN")
        .sum()
        .reset_index()
    )

    df = pd.merge(sales_weekly, acq_weekly, on="date", how="outer").fillna(0)
    df["roas"] = df.apply(
        lambda row: row["sales_total"] / row["acquisition_expense_total"]
        if row["acquisition_expense_total"] > 0 else 0,
        axis=1,
    )

    return df.sort_values("date").reset_index(drop=True)


def get_test_last_6_weeks_roas() -> pd.DataFrame:
    df = get_test_roas_history()
    if df.empty:
        return df

    df = df[
        (df["sales_total"] > 0) &
        (df["acquisition_expense_total"] > 0)
    ].copy()

    return df.sort_values("date").tail(6).reset_index(drop=True)