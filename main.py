import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from datetime import date
from pathlib import Path

from database import Base, engine
from settings import get_shinnyskin_password
from services.amazon_upload_service import upload_amazon_files_to_db
from services.units_service import (
    PRODUCT_COLUMNS,
    get_units_history,
    prepare_units_dataframe,
    upsert_units_from_dataframe,
)
from services.sales_service import prepare_sales_dataframe, upsert_sales_from_dataframe, get_sales_history
from services.expenses_service import prepare_expenses_dataframe, upsert_expenses_from_dataframe, get_expenses_history
from services.banks_service import (
    prepare_banks_dataframe,
    upsert_banks_from_dataframe,
    upsert_banks_row,
    get_latest_banks_date,
    get_missing_bank_week_end_dates_since_latest,
)
from services.forecast_run_service import (
    run_sales_forecast,
    run_units_forecast,
    run_cashflow_projection,
)
from services.ml_update_service import update_ml_last_weeks
from services.acquisition_expense_service import (
    upsert_acquisition_expense_row,
    get_latest_acquisition_expense_date,
    get_missing_week_end_dates_since_latest,
)
from services.roas_service import get_last_6_weeks_roas_by_mode
from services.test_data_service import get_test_expenses_daily_totals

st.set_page_config(page_title="Forecast Dashboard", layout="wide")
Base.metadata.create_all(bind=engine)

COLUMN_LABELS = {
    "date": "Date", "forecast": "Forecast", "min": "Min", "max": "Max",
    "sales_total": "Sales", "units_total": "Units", "expenses_total": "Expenses",
    "acquisition_expense_total": "Acquisition Expense", "roas": "ROAS",
    "sales_forecast": "Sales Forecast", "sales_min": "Sales Min", "sales_max": "Sales Max",
    "expenses": "Expenses", "net_income": "Net Income", "net_income_min": "Net Income Min",
    "net_income_max": "Net Income Max", "balance": "Balance", "balance_min": "Balance Min",
    "balance_max": "Balance Max", "projected_sales": "Sales Forecast",
    "projected_sales_min": "Sales Min", "projected_sales_max": "Sales Max",
    "projected_expenses": "Expenses", "projected_net_income": "Net Income",
    "projected_net_income_min": "Net Income Min", "projected_net_income_max": "Net Income Max",
    "projected_bank_balance": "Balance", "projected_bank_balance_min": "Balance Min",
    "projected_bank_balance_max": "Balance Max",
}

UPLOAD_CONFIG = {
    "Units": (prepare_units_dataframe, upsert_units_from_dataframe, "update_units_csv_uploader", "Load Units into DB"),
    "Sales": (prepare_sales_dataframe, upsert_sales_from_dataframe, "update_sales_csv_uploader", "Load Sales into DB"),
    "Expenses": (prepare_expenses_dataframe, upsert_expenses_from_dataframe, "update_expenses_csv_uploader", "Load Expenses into DB"),
    "Banks": (prepare_banks_dataframe, upsert_banks_from_dataframe, "update_banks_csv_uploader", "Load Banks into DB"),
}

for key, value in {"app_mode": None, "authenticated_shinny": False}.items():
    st.session_state.setdefault(key, value)


def inject_dashboard_theme():
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 3rem;
            padding-bottom: 2rem;
            max-width: 1280px;
        }

        .metric-card {
            background: linear-gradient(180deg, #131A2A 0%, #0E1422 100%);
            border: 1px solid rgba(231,184,194,0.20);
            border-radius: 18px;
            padding: 18px 20px;
            min-height: 112px;
        }

        .metric-label {
            color: #CFA7B0;
            font-size: 0.9rem;
            margin-bottom: 8px;
        }

        .metric-value {
            color: white;
            font-size: 1.8rem;
            font-weight: 700;
            line-height: 1.1;
        }

        .metric-sub {
            color: #A8AAB3;
            font-size: 0.82rem;
            margin-top: 6px;
        }

        .section-card {
            background: linear-gradient(180deg, #111727 0%, #0B1020 100%);
            border: 1px solid rgba(231,184,194,0.15);
            border-radius: 18px;
            padding: 20px;
        }

        .insight-box {
            background: rgba(231,184,194,0.10);
            border: 1px solid rgba(231,184,194,0.25);
            border-radius: 16px;
            padding: 16px 18px;
        }

        div.stButton > button {
            width: 100%;
            border-radius: 12px;
            height: 46px;
            font-weight: 600;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_upload_section(title: str, uploader_key: str, prepare_func, upsert_func, button_label: str):
    st.subheader(title)
    uploaded_file = st.file_uploader(f"Upload {title} CSV", type=["csv"], key=uploader_key)

    if uploaded_file is None:
        return

    try:
        df = prepare_func(uploaded_file)
        st.write("Preview of uploaded data:")
        st.dataframe(df, use_container_width=True)

        if st.button(button_label, key=f"{uploader_key}_button"):
            inserted, updated = upsert_func(df)
            st.success(f"Upload completed successfully. Inserted: {inserted} rows. Updated: {updated} rows.")
    except Exception as e:
        st.error(f"Error processing file: {e}")


def render_forecast_chart(df: pd.DataFrame, x_col: str, y_col: str, min_col: str, max_col: str):
    if df.empty:
        st.warning("No chart data available.")
        return

    chart_df = df[[x_col, y_col, min_col, max_col]].rename(
        columns={x_col: "date", y_col: "forecast", min_col: "min", max_col: "max"}
    )
    chart_df["date"] = pd.to_datetime(chart_df["date"])
    st.line_chart(chart_df.set_index("date"), use_container_width=True)


def render_forecast_band_chart(
    df: pd.DataFrame,
    x_col: str = "date",
    y_col: str = "forecast",
    min_col: str = "min",
    max_col: str = "max",
    real_col: str = "real",
    title: str = "",
):
    if df is None or df.empty:
        st.warning("No chart data available.")
        return

    chart_df = df.copy()
    chart_df[x_col] = pd.to_datetime(chart_df[x_col])

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=chart_df[x_col],
            y=chart_df[max_col],
            mode="lines",
            line=dict(width=0),
            showlegend=False,
            hoverinfo="skip",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=chart_df[x_col],
            y=chart_df[min_col],
            mode="lines",
            line=dict(width=0),
            fill="tonexty",
            fillcolor="rgba(231,184,194,0.18)",
            name="Range",
            hoverinfo="skip",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=chart_df[x_col],
            y=chart_df[y_col],
            mode="lines",
            name="Forecast",
            line=dict(color="#E7B8C2", width=3),
        )
    )

    if real_col in chart_df.columns:
        real_df = chart_df[chart_df[real_col].notna()].copy()
        if not real_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=real_df[x_col],
                    y=real_df[real_col],
                    mode="markers",
                    name="Real",
                    marker=dict(size=8, color="white"),
                )
            )

    fig.update_layout(
        title=title,
        template="plotly_dark",
        height=420,
        margin=dict(l=20, r=20, t=50, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis_title="",
        yaxis_title="",
        legend_title="",
    )

    st.plotly_chart(fig, use_container_width=True)


def render_cashflow_debug_table(cashflow_projection: pd.DataFrame):
    if cashflow_projection.empty:
        return

    debug_cols = [
        "date", "sales_forecast", "sales_min", "sales_max", "expenses", "net_income",
        "net_income_min", "net_income_max", "balance", "balance_min", "balance_max",
    ]
    rename_map = {
        "ds": "date", "projected_sales": "sales_forecast", "projected_sales_min": "sales_min",
        "projected_sales_max": "sales_max", "projected_expenses": "expenses",
        "projected_net_income": "net_income", "projected_net_income_min": "net_income_min",
        "projected_net_income_max": "net_income_max", "projected_bank_balance": "balance",
        "projected_bank_balance_min": "balance_min", "projected_bank_balance_max": "balance_max",
    }

    with st.expander("Show detailed cash flow debug table"):
        render_pretty_table(cashflow_projection.rename(columns=rename_map)[debug_cols].copy())


def render_ml_update_section(section_key: str):
    st.markdown("### Update Database")
    st.caption("Only updates Mercado Libre data for marketplace: ml. CSV upload remains available for all other sources.")

    weeks = st.slider("Weeks to update", min_value=1, max_value=52, value=4, step=1, key=f"{section_key}_weeks_slider")

    if st.button("Update Mercado Libre Data", key=f"{section_key}_ml_update_button"):
        try:
            summary = update_ml_last_weeks(weeks)
            st.success("Mercado Libre update completed successfully.")
            st.write(f"Date range updated: **{summary['start_date']}** to **{summary['end_date']}**")
            for table in ["units", "sales"]:
                st.write(f"{table.title()} rows processed: **{summary[f'{table}_rows']}**")
                st.write(f"{table.title()} inserted: **{summary[f'{table}_inserted']}**, updated: **{summary[f'{table}_updated']}**")
        except Exception as e:
            st.error(f"Error updating Mercado Libre data: {e}")


def render_update_data():
    inject_dashboard_theme()
    st.subheader("Update Data")

    st.markdown("### Mercado Libre")
    render_ml_update_section("update_data")

    st.markdown("---")
    render_amazon_upload_section()

    st.markdown("---")
    render_banks_update_section()

    st.markdown("---")
    st.markdown("### Manual Uploads")

    upload_option = st.radio(
        "Choose what you want to update",
        options=["Units", "Sales", "Expenses", "Banks"],
        horizontal=True,
        key="update_data_upload_option",
    )

    if upload_option == "Units":
        render_upload_section(
            title="Units",
            uploader_key="update_units_csv_uploader",
            prepare_func=prepare_units_dataframe,
            upsert_func=upsert_units_from_dataframe,
            button_label="Load Units into DB",
        )

    elif upload_option == "Sales":
        render_upload_section(
            title="Sales",
            uploader_key="update_sales_csv_uploader",
            prepare_func=prepare_sales_dataframe,
            upsert_func=upsert_sales_from_dataframe,
            button_label="Load Sales into DB",
        )

    elif upload_option == "Expenses":
        render_upload_section(
            title="Expenses",
            uploader_key="update_expenses_csv_uploader",
            prepare_func=prepare_expenses_dataframe,
            upsert_func=upsert_expenses_from_dataframe,
            button_label="Load Expenses into DB",
        )

    elif upload_option == "Banks":
        render_upload_section(
            title="Banks",
            uploader_key="update_banks_csv_uploader",
            prepare_func=prepare_banks_dataframe,
            upsert_func=upsert_banks_from_dataframe,
            button_label="Load Banks into DB",
        )


def _forecast_display(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "ds": "date",
            "yhat": "forecast",
            "yhat_lower": "min",
            "yhat_upper": "max",
            "real": "real",
        }
    )[["date", "forecast", "min", "max", "real"]]


def _render_forecast_result(title: str, forecast_df: pd.DataFrame):
    st.markdown(f"### {title} Table")

    if forecast_df.empty:
        st.warning(f"No {title.lower()} available.")
        return

    display_df = _forecast_display(forecast_df)
    render_pretty_table(display_df)

    st.markdown(f"### {title} Chart")
    render_forecast_band_chart(display_df, title=title)



def _get_upcoming_expenses_by_mode(mode: str) -> pd.DataFrame:
    if mode == "test":
        df = get_test_expenses_daily_totals()
        if df.empty:
            return pd.DataFrame(columns=["date", "concept", "total"])
        df = df.rename(columns={"expenses_total": "total"})
        df["concept"] = "Sample future expense"
    else:
        df = get_expenses_history()

    if df.empty:
        return pd.DataFrame(columns=["date", "concept", "total"])

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    out["total"] = pd.to_numeric(out["total"], errors="coerce").fillna(0).round(2)
    today = pd.Timestamp(date.today()).normalize()
    return out[out["date"] >= today][["date", "concept", "total"]].sort_values(["date", "concept"]).reset_index(drop=True)


def _get_units_history_by_mode(mode: str) -> pd.DataFrame:
    if mode == "test":
        path = Path("sample_data") / "example_units.csv"
        if not path.exists():
            return pd.DataFrame(columns=["date", "mkp_name", *PRODUCT_COLUMNS])
        df = pd.read_csv(path)
    else:
        df = get_units_history()

    if df.empty:
        return pd.DataFrame(columns=["date", "mkp_name", *PRODUCT_COLUMNS])

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    out["mkp_name"] = out["mkp_name"].astype(str).str.strip().str.lower()

    for col in PRODUCT_COLUMNS:
        if col not in out.columns:
            out[col] = 0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

    return out[["date", "mkp_name", *PRODUCT_COLUMNS]].sort_values(["date", "mkp_name"]).reset_index(drop=True)
def _get_sales_history_by_mode(mode: str) -> pd.DataFrame:
    if mode == "test":
        path = Path("sample_data") / "example_sales.csv"
        if not path.exists():
            return pd.DataFrame(columns=["date", "mkp_name", *PRODUCT_COLUMNS])
        df = pd.read_csv(path)
    else:
        df = get_sales_history()

    if df.empty:
        return pd.DataFrame(columns=["date", "mkp_name", *PRODUCT_COLUMNS])

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    out["mkp_name"] = out["mkp_name"].astype(str).str.strip().str.lower()

    for col in PRODUCT_COLUMNS:
        if col not in out.columns:
            out[col] = 0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

    return out[["date", "mkp_name", *PRODUCT_COLUMNS]].sort_values(["date", "mkp_name"]).reset_index(drop=True)
def _latest_completed_row(df: pd.DataFrame, date_col: str = "date") -> pd.Series | None:
    if df is None or df.empty or date_col not in df.columns:
        return None

    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col])
    out = out.sort_values(date_col).dropna(subset=[date_col])
    return None if out.empty else out.iloc[-1]


def _next_forecast_value(forecast_display: pd.DataFrame) -> float:
    if forecast_display is None or forecast_display.empty:
        return 0.0

    out = forecast_display.copy()
    out["forecast"] = pd.to_numeric(out.get("forecast"), errors="coerce")
    if "real" in out.columns:
        future = out[out["real"].isna()].copy()
        if not future.empty:
            return float(future["forecast"].iloc[0])

    return float(out["forecast"].dropna().iloc[-1]) if out["forecast"].notna().any() else 0.0


def _get_summary_roas_kpi(mode: str) -> dict:
    roas_df = get_last_6_weeks_roas_by_mode(mode=mode)
    if roas_df.empty:
        return {"date": None, "sales_total": 0.0, "acquisition_expense_total": 0.0, "roas": 0.0}

    latest = roas_df.sort_values("date").iloc[-1]
    return {
        "date": pd.to_datetime(latest["date"]).date(),
        "sales_total": float(latest.get("sales_total", 0) or 0),
        "acquisition_expense_total": float(latest.get("acquisition_expense_total", 0) or 0),
        "roas": float(latest.get("roas", 0) or 0),
    }


def _render_summary_metric_cards(
    sales_history: pd.DataFrame,
    sales_display: pd.DataFrame,
    cashflow_history: pd.DataFrame,
    cashflow_display: pd.DataFrame,
    upcoming_expenses: pd.DataFrame,
):
    last_sales_row = _latest_completed_row(sales_history.rename(columns={"ds": "date"}))
    last_cashflow_row = _latest_completed_row(cashflow_history)

    last_week_sales = float(last_sales_row.get("y", 0) or 0) if last_sales_row is not None else 0
    next_week_sales_forecast = _next_forecast_value(sales_display)
    last_week_balance = float(last_cashflow_row.get("banks_total", 0) or 0) if last_cashflow_row is not None else 0
    next_projected_balance = _next_forecast_value(cashflow_display)
    future_expenses_total = float(upcoming_expenses["total"].sum()) if not upcoming_expenses.empty else 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Last week sales", f"${last_week_sales:,.0f}")
    with col2:
        st.metric("Next week forecasted sales", f"${next_week_sales_forecast:,.0f}")
    with col3:
        st.metric("Last week real balance", f"${last_week_balance:,.0f}")
    with col4:
        st.metric("Next projected balance", f"${next_projected_balance:,.0f}", help=f"All upcoming saved expenses: ${future_expenses_total:,.0f}")


def _last_saved_week_slice(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Timestamp | None, pd.Timestamp | None]:
    if df is None or df.empty or "date" not in df.columns:
        return pd.DataFrame(), None, None

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    last_date = out["date"].max()
    week_start = last_date - pd.Timedelta(days=6)
    return out[(out["date"] >= week_start) & (out["date"] <= last_date)].copy(), week_start, last_date


def _render_bar_chart(df: pd.DataFrame, x_col: str, y_col: str, title: str, value_prefix: str = ""):
    if df.empty:
        st.info("No data available for this chart.")
        return

    fig = go.Figure(
        go.Bar(
            x=df[x_col],
            y=df[y_col],
            text=df[y_col],
            texttemplate=f"{value_prefix}%{{text:,.0f}}",
            textposition="outside",
        )
    )
    fig.update_layout(
        title=title,
        template="plotly_dark",
        height=380,
        margin=dict(l=20, r=20, t=50, b=60),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis_title="",
        yaxis_title="",
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_roas_chart(mode: str):
    roas_df = get_last_6_weeks_roas_by_mode(mode=mode)
    st.markdown("#### ROAS")

    if roas_df.empty:
        st.warning("No ROAS data available.")
        return

    chart_df = roas_df.copy().sort_values("date")
    chart_df["date"] = pd.to_datetime(chart_df["date"])
    chart_df["roas"] = pd.to_numeric(chart_df["roas"], errors="coerce").fillna(0)

    latest = chart_df.iloc[-1]
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Latest ROAS", f"{latest['roas']:.2f}x")
    with col2:
        st.metric("Latest sales", f"${float(latest.get('sales_total', 0) or 0):,.0f}")
    with col3:
        st.metric("Latest acquisition expense", f"${float(latest.get('acquisition_expense_total', 0) or 0):,.0f}")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=chart_df["date"],
            y=chart_df["roas"],
            mode="lines+markers+text",
            text=chart_df["roas"].round(2),
            textposition="top center",
            name="ROAS",
            line=dict(color="#E7B8C2", width=3),
            marker=dict(size=8),
        )
    )
    fig.update_layout(
        title="ROAS - Last 6 Saved Weeks",
        template="plotly_dark",
        height=380,
        margin=dict(l=20, r=20, t=50, b=40),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis_title="",
        yaxis_title="ROAS",
        legend_title="",
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Show ROAS table"):
        render_pretty_table(chart_df[["date", "sales_total", "acquisition_expense_total", "roas"]], percent_cols=["roas"])


def _render_summary_kpis(mode: str):
    units_df = _get_units_history_by_mode(mode)
    sales_df = _get_sales_history_by_mode(mode)

    st.markdown("### KPI Snapshot")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Units by product")
        last_week_units, week_start, last_date = _last_saved_week_slice(units_df)

        if last_week_units.empty:
            st.warning("No units data available.")
        else:
            units_by_product = (
                last_week_units[PRODUCT_COLUMNS]
                .sum()
                .reset_index()
                .rename(columns={"index": "product", 0: "units"})
            )
            units_by_product["units"] = pd.to_numeric(units_by_product["units"], errors="coerce").fillna(0)
            units_by_product = units_by_product[units_by_product["units"] > 0].sort_values("units", ascending=False)

            if units_by_product.empty:
                st.info("No unit movement in the last saved week.")
            else:
                st.caption(f"Last saved week: {week_start.date()} to {last_date.date()}")
                _render_bar_chart(units_by_product, "product", "units", "Units by Product - Last Saved Week")
                with st.expander("Show units by product table"):
                    render_pretty_table(units_by_product, date_col="", decimals=0)

    with col2:
        st.markdown("#### Income by marketplace")
        last_week_sales, sales_week_start, sales_last_date = _last_saved_week_slice(sales_df)

        if last_week_sales.empty:
            st.warning("No sales data available.")
        else:
            last_week_sales = last_week_sales.copy()
            last_week_sales["income"] = last_week_sales[PRODUCT_COLUMNS].sum(axis=1)
            income_by_marketplace = (
                last_week_sales
                .groupby("mkp_name", as_index=False)["income"]
                .sum()
                .sort_values("income", ascending=False)
            )
            income_by_marketplace = income_by_marketplace[income_by_marketplace["income"] > 0]

            if income_by_marketplace.empty:
                st.info("No marketplace income in the last saved week.")
            else:
                st.caption(f"Last saved week: {sales_week_start.date()} to {sales_last_date.date()}")
                _render_bar_chart(income_by_marketplace, "mkp_name", "income", "Income by Marketplace - Last Saved Week", value_prefix="$")
                with st.expander("Show income by marketplace table"):
                    render_pretty_table(income_by_marketplace, date_col="")

    st.markdown("---")
    _render_roas_chart(mode)
def render_summary_dashboard():
    inject_dashboard_theme()
    mode = st.session_state.get("app_mode", "shinny")

    st.subheader("Summary Dashboard")
    st.caption("Weekly view: past real performance, forecasted sales, projected cash flow, and all upcoming saved expenses.")

    col1, col2 = st.columns(2)
    with col1:
        past_weeks = st.number_input(
            "Past weeks",
            min_value=1,
            max_value=52,
            value=4,
            step=1,
            key="summary_past_weeks",
        )
    with col2:
        forecasted_period = st.number_input(
            "Forecasted period",
            min_value=1,
            max_value=52,
            value=4,
            step=1,
            key="summary_forecasted_period",
        )

    try:
        sales_history, sales_forecast = run_sales_forecast(
            periods=forecasted_period,
            freq="weekly",
            mode=mode,
            past_periods_to_show=past_weeks,
        )
        cashflow_history, cashflow_projection = run_cashflow_projection(
            periods=forecasted_period,
            freq="weekly",
            mode=mode,
            past_periods_to_show=past_weeks,
        )
        upcoming_expenses = _get_upcoming_expenses_by_mode(mode)
    except Exception as e:
        st.error(f"Error loading summary dashboard: {e}")
        return

    sales_display = pd.DataFrame(columns=["date", "forecast", "min", "max", "real"])
    if not sales_forecast.empty:
        sales_display = sales_forecast.rename(
            columns={"ds": "date", "yhat": "forecast", "yhat_lower": "min", "yhat_upper": "max", "real": "real"}
        )[["date", "forecast", "min", "max", "real"]]

    cashflow_display = pd.DataFrame(columns=["date", "forecast", "min", "max", "real"])
    if not cashflow_projection.empty:
        cashflow_display = cashflow_projection.rename(
            columns={
                "ds": "date",
                "projected_bank_balance": "forecast",
                "projected_bank_balance_min": "min",
                "projected_bank_balance_max": "max",
                "real_balance": "real",
            }
        )[["date", "forecast", "min", "max", "real"]]

    _render_summary_metric_cards(
        sales_history=sales_history,
        sales_display=sales_display,
        cashflow_history=cashflow_history,
        cashflow_display=cashflow_display,
        upcoming_expenses=upcoming_expenses,
    )

    _render_summary_kpis(mode)

    st.markdown("---")
    st.markdown("### Sales: Past Real vs Forecast")
    if sales_display.empty:
        st.warning("No sales forecast available.")
    else:
        render_forecast_band_chart(sales_display, title="Sales Summary Forecast")
        with st.expander("Show sales table"):
            render_pretty_table(sales_display)

    st.markdown("### Cash Flow: Real Balances vs Projection")
    if cashflow_display.empty:
        st.warning("No cash flow projection available.")
    else:
        render_forecast_band_chart(cashflow_display, title="Cash Flow Summary Projection")
        with st.expander("Show cash flow table"):
            render_pretty_table(cashflow_display)

    st.markdown("### Upcoming Expenses")
    if upcoming_expenses.empty:
        st.info("No upcoming expenses saved.")
    else:
        render_pretty_table(upcoming_expenses)


def render_sales_forecast():
    inject_dashboard_theme()
    st.subheader("Sales Forecast")

    freq = st.selectbox(
        "Data frequency",
        options=["daily", "weekly", "monthly"],
        index=0,
        key="sales_forecast_frequency",
    )

    periods = st.number_input(
        "Forecast periods",
        min_value=1,
        max_value=120,
        value=15,
        step=1,
        key="sales_forecast_periods",
    )

    if st.button("Run Sales and Units Forecast", key="run_sales_units_forecast_button"):
        st.session_state["run_sales_units_forecast"] = True

    if st.session_state.get("run_sales_units_forecast"):
        try:
            mode = st.session_state.get("app_mode", "shinny")

            _, sales_forecast = run_sales_forecast(
                periods=periods,
                freq=freq,
                mode=mode,
            )

            _, units_forecast = run_units_forecast(
                periods=periods,
                freq=freq,
                mode=mode,
            )

            st.markdown("### Sales Forecast Table")
            if sales_forecast.empty:
                st.warning("No sales forecast available.")
            else:
                sales_display = sales_forecast.rename(
                    columns={
                        "ds": "date",
                        "yhat": "forecast",
                        "yhat_lower": "min",
                        "yhat_upper": "max",
                        "real": "real",
                    }
                )

                sales_cols = ["date", "forecast", "min", "max"]
                if "real" in sales_display.columns:
                    sales_cols.append("real")

                sales_display = sales_display[sales_cols]

                render_pretty_table(sales_display)
                render_forecast_band_chart(
                    sales_display,
                    x_col="date",
                    y_col="forecast",
                    min_col="min",
                    max_col="max",
                    real_col="real",
                    title="Sales Forecast",
                )

            st.markdown("### Units Forecast Table")
            if units_forecast.empty:
                st.warning("No units forecast available.")
            else:
                units_display = units_forecast.rename(
                    columns={
                        "ds": "date",
                        "yhat": "forecast",
                        "yhat_lower": "min",
                        "yhat_upper": "max",
                        "real": "real",
                    }
                )

                units_cols = ["date", "forecast", "min", "max"]
                if "real" in units_display.columns:
                    units_cols.append("real")

                units_display = units_display[units_cols]

                render_pretty_table(units_display)
                render_forecast_band_chart(
                    units_display,
                    x_col="date",
                    y_col="forecast",
                    min_col="min",
                    max_col="max",
                    real_col="real",
                    title="Units Forecast",
                )

        except Exception as e:
            st.error(f"Error running sales forecast: {e}")


def render_cashflow():
    inject_dashboard_theme()
    st.subheader("Cash Flow")

    freq = st.selectbox(
        "Data frequency",
        options=["daily", "weekly", "monthly"],
        index=0,
        key="cashflow_frequency",
    )

    periods = st.number_input(
        "Projection periods",
        min_value=1,
        max_value=120,
        value=15,
        step=1,
        key="cashflow_periods",
    )

    if st.button("Run Cash Flow Projection", key="run_cashflow_projection_button"):
        st.session_state["run_cashflow_projection"] = True

    if st.session_state.get("run_cashflow_projection"):
        try:
            mode = st.session_state.get("app_mode", "shinny")

            _, cashflow_projection = run_cashflow_projection(
                periods=periods,
                freq=freq,
                mode=mode,
            )

            if cashflow_projection.empty:
                st.warning("No cash flow projection available.")
                return

            cashflow_display = cashflow_projection.rename(
                columns={
                    "ds": "date",
                    "projected_bank_balance": "forecast",
                    "projected_bank_balance_min": "min",
                    "projected_bank_balance_max": "max",
                    "real_balance": "real",
                }
            )

            cash_cols = ["date", "forecast", "min", "max"]
            if "real" in cashflow_display.columns:
                cash_cols.append("real")

            cashflow_display = cashflow_display[cash_cols]

            st.markdown("### Cash Flow Projection Table")
            render_pretty_table(cashflow_display)

            render_cashflow_debug_table(cashflow_projection)

            st.markdown("### Cash Flow Projection Chart")
            render_forecast_band_chart(
                cashflow_display,
                x_col="date",
                y_col="forecast",
                min_col="min",
                max_col="max",
                real_col="real",
                title="Cash Flow Projection",
            )

        except Exception as e:
            st.error(f"Error running cash flow projection: {e}")


def render_roas():
    mode = st.session_state.get("app_mode", "shinny")
    st.subheader("ROAS")

    if mode == "shinny":
        latest_date = get_latest_acquisition_expense_date()
        if latest_date is None:
            st.info("No acquisition expense data saved yet.")
        else:
            st.write(f"Latest date with data: **{latest_date}**")

        if st.button("Update data", key="roas_update_data_button"):
            st.session_state.roas_show_update = True

        if st.session_state.get("roas_show_update"):
            selected_date = _select_update_date("roas", "Select week date to update", get_missing_week_end_dates_since_latest(), "Choose any date")
            values = _number_inputs(
                prefix="roas",
                fields=["Amazon", "Mercado Libre", "Facebook", "Tiktok", "Google", "UGC_y_Colab", "Otros"],
                min_value=0.0,
                step=100.0,
            )
            if st.button("Save acquisition expense", key="roas_save_button"):
                inserted, updated = upsert_acquisition_expense_row(
                    row_date=selected_date,
                    Amazon=values["Amazon"], Mercado_Libre=values["Mercado Libre"],
                    Facebook=values["Facebook"], Tiktok=values["Tiktok"], Google=values["Google"],
                    UGC_y_Colab=values["UGC_y_Colab"], Otros=values["Otros"],
                )
                st.success(f"Saved successfully. Inserted: {inserted}, Updated: {updated}")
    else:
        st.info("Test mode uses static sample data.")

    st.markdown("### ROAS - Last 6 weeks")
    roas_df = get_last_6_weeks_roas_by_mode(mode=mode)

    if roas_df.empty:
        st.warning("No ROAS data available yet.")
        return

    roas_display = roas_df.copy()
    roas_display["date"] = pd.to_datetime(roas_display["date"])
    render_pretty_table(roas_display[["date", "sales_total", "acquisition_expense_total", "roas"]], percent_cols=["roas"])
    st.line_chart(roas_display[["date", "roas"]].set_index("date"), use_container_width=True)


def _select_update_date(prefix: str, label: str, missing_dates: list, manual_label: str):
    options = ["Manual date"] + [d.isoformat() for d in missing_dates]
    selected_option = st.selectbox(label, options=options, key=f"{prefix}_date_selector")
    if selected_option == "Manual date":
        return st.date_input(manual_label, value=date.today(), key=f"{prefix}_manual_date_input")
    return pd.to_datetime(selected_option).date()


def _number_inputs(prefix: str, fields: list[str], min_value=None, step=100.0) -> dict:
    values = {}
    for field in fields:
        key = f"{prefix}_{field.lower().replace(' ', '_')}"
        kwargs = {"value": 0.0, "step": step, "key": key}
        if min_value is not None:
            kwargs["min_value"] = min_value
        values[field] = st.number_input(field, **kwargs)
    return values


def render_banks_update_section():
    st.markdown("### Banks Weekly Update")
    latest_date = get_latest_banks_date()
    if latest_date is None:
        st.info("No bank data saved yet.")
    else:
        st.write(f"Latest date with data: **{latest_date}**")

    if st.button("Update banks data", key="banks_update_data_button"):
        st.session_state.banks_show_update = True

    if not st.session_state.get("banks_show_update"):
        return

    selected_date = _select_update_date("banks", "Select bank week date to update", get_missing_bank_week_end_dates_since_latest(), "Choose any bank date")
    values = _number_inputs("banks", ["BBVA", "BRG", "MP", "MP Liberar", "Shop", "LVP", "Amazon"], step=1000.0)

    if st.button("Save banks data", key="banks_save_button"):
        inserted, updated = upsert_banks_row(
            row_date=selected_date,
            bbva=values["BBVA"], brg=values["BRG"], mp=values["MP"],
            mp_liberar=values["MP Liberar"], shop=values["Shop"], lvp=values["LVP"], coppel=values["Amazon"],
        )
        st.success(f"Saved successfully. Inserted: {inserted}, Updated: {updated}")


def render_amazon_upload_section():
    st.markdown("### Amazon TXT Upload")
    st.caption("Upload one or more Amazon order report .txt files. This updates sales and units for marketplace: amzn.")
    uploaded_files = st.file_uploader("Upload Amazon TXT files", type=["txt"], accept_multiple_files=True, key="amazon_txt_uploader")

    if uploaded_files and st.button("Load Amazon files into DB", key="load_amazon_txt_button"):
        try:
            summary = upload_amazon_files_to_db(uploaded_files)
            st.success("Amazon data uploaded successfully.")
            for table in ["units", "sales"]:
                st.write(f"{table.title()} rows processed: **{summary[f'{table}_rows']}**")
                st.write(f"{table.title()} inserted: **{summary[f'{table}_inserted']}**, updated: **{summary[f'{table}_updated']}**")
            st.markdown("#### Units preview")
            st.dataframe(summary["df_units"], use_container_width=True)
            st.markdown("#### Sales preview")
            st.dataframe(summary["df_sales"], use_container_width=True)
        except Exception as e:
            st.error(f"Error uploading Amazon files: {e}")


def render_mode_selector():
    st.title("Select Environment")
    st.caption("Choose the environment you want to access.")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Shinny Skin", key="select_shinny", use_container_width=True):
            st.session_state.app_mode = "shinny"
            st.rerun()
    with col2:
        if st.button("Test", key="select_test", use_container_width=True):
            st.session_state.app_mode = "test"
            st.rerun()


def render_shinny_login():
    st.title("Shinny Skin Access")
    st.caption("Enter password to access the private environment.")
    password = st.text_input("Password", type="password", key="shinny_password_input")
    col1, col2 = st.columns([1, 1])

    with col1:
        if st.button("Enter", key="shinny_enter", use_container_width=True):
            if password == get_shinnyskin_password():
                st.session_state.authenticated_shinny = True
                st.rerun()
            st.error("Incorrect password.")
    with col2:
        if st.button("Back", key="shinny_back", use_container_width=True):
            _reset_navigation()
            st.session_state.shinny_password_input = ""
            st.rerun()


def render_mode_banner():
    mode = st.session_state.get("app_mode")
    if mode in ["shinny", "test"]:
        st.info(f"Mode: {'Shinny Skin' if mode == 'shinny' else 'Test'}")


def _reset_navigation():
    st.session_state.app_mode = None
    st.session_state.authenticated_shinny = False
    st.session_state.selected_option = None


def render_home():
    st.markdown(
        """
        <style>
        .block-container {padding-top: 4rem; padding-bottom: 2rem; max-width: 1200px;}
        div.stButton > button {width: 100%; border-radius: 10px; height: 48px; font-weight: 600;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    mode = st.session_state.get("app_mode")
    st.title("Forecast Dashboard")
    st.caption("Update your operational data and monitor forecasts, cash flow, and ROAS.")
    render_mode_banner()

    col_back, _ = st.columns([1, 4])
    with col_back:
        if st.button("Back to selector", use_container_width=True):
            _reset_navigation()
            st.rerun()

    st.markdown("### Navigation")
    nav_options = [
        ("Summary Dashboard", "summary_dashboard", True),
        ("Update Data", "update_data", mode == "shinny"),
        ("Sales Forecast", "sales_forecast", True),
        ("Cash Flow", "cash_flow", True),
        ("ROAS", "roas", True),
    ]
    for col, (label, option, enabled) in zip(st.columns(5), nav_options):
        with col:
            if enabled and st.button(label, use_container_width=True):
                st.session_state.selected_option = option

    st.markdown("---")
    if st.session_state.get("selected_option") is None:
        st.session_state.selected_option = "summary_dashboard"


def route_page():
    routes = {
        "summary_dashboard": render_summary_dashboard,
        "sales_forecast": render_sales_forecast,
        "cash_flow": render_cashflow,
        "update_data": render_update_data,
        "roas": render_roas,
    }
    route = routes.get(st.session_state.get("selected_option"))
    if route:
        route()
    else:
        st.info("Choose an option to continue.")


def render_pretty_table(df: pd.DataFrame, date_col: str = "date", percent_cols: list[str] | None = None, decimals: int = 2):
    if df.empty:
        st.warning("No data available.")
        return

    out = df.copy()
    if date_col in out.columns:
        out[date_col] = pd.to_datetime(out[date_col]).dt.strftime("%Y-%m-%d")
        out = out.set_index(date_col)

    out = out.rename(columns=COLUMN_LABELS)
    percent_labels = [COLUMN_LABELS.get(col, col) for col in (percent_cols or [])]
    format_dict = {
        col: ("{:.4f}" if col in percent_labels else f"{{:,.{decimals}f}}")
        for col in out.columns
        if pd.api.types.is_numeric_dtype(out[col])
    }
    st.dataframe(out.style.format(format_dict), use_container_width=True)


def main():
    mode = st.session_state.get("app_mode")
    if mode is None:
        render_mode_selector()
        return
    if mode == "shinny" and not st.session_state.get("authenticated_shinny", False):
        render_shinny_login()
        return
    render_home()
    route_page()


if __name__ == "__main__":
    main()
