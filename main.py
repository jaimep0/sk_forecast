import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from datetime import date

from database import Base, engine
from settings import get_shinnyskin_password
from services.amazon_upload_service import upload_amazon_files_to_db
from services.units_service import prepare_units_dataframe, upsert_units_from_dataframe
from services.sales_service import prepare_sales_dataframe, upsert_sales_from_dataframe
from services.expenses_service import prepare_expenses_dataframe, upsert_expenses_from_dataframe
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
    title: str = "",
):
    if df.empty:
        st.warning("No chart data available.")
        return

    chart_df = df.copy()
    chart_df[x_col] = pd.to_datetime(chart_df[x_col])

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=chart_df[x_col], y=chart_df[max_col], mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=chart_df[x_col], y=chart_df[min_col], mode="lines", line=dict(width=0), fill="tonexty", fillcolor="rgba(100, 149, 237, 0.18)", name="Range", hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=chart_df[x_col], y=chart_df[y_col], mode="lines", name="Forecast", line=dict(width=3)))
    fig.update_layout(title=title, template="plotly_dark", height=420, margin=dict(l=20, r=20, t=50, b=20), xaxis_title="", yaxis_title="", legend_title="")
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
    st.subheader("Update Data")
    st.markdown("### Mercado Libre")
    render_ml_update_section("update_data")
    st.markdown("---")
    render_amazon_upload_section()
    st.markdown("---")
    render_banks_update_section()
    st.markdown("---")
    st.markdown("### Manual Uploads")

    upload_option = st.radio("Choose what you want to update", options=list(UPLOAD_CONFIG), horizontal=True, key="update_data_upload_option")
    prepare_func, upsert_func, uploader_key, button_label = UPLOAD_CONFIG[upload_option]
    render_upload_section(upload_option, uploader_key, prepare_func, upsert_func, button_label)


def _forecast_display(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={"ds": "date", "yhat": "forecast", "yhat_lower": "min", "yhat_upper": "max"})[["date", "forecast", "min", "max"]]


def _render_forecast_result(title: str, forecast_df: pd.DataFrame):
    st.markdown(f"### {title} Table")
    if forecast_df.empty:
        st.warning(f"No {title.lower()} available.")
        return

    display_df = _forecast_display(forecast_df)
    render_pretty_table(display_df)
    st.markdown(f"### {title} Chart")
    render_forecast_band_chart(display_df, title=title)


def render_sales_forecast():
    st.subheader("Sales Forecast")
    freq = st.selectbox("Data frequency", options=["daily", "weekly", "monthly"], index=0, key="sales_forecast_frequency")
    periods = st.number_input("Forecast periods", min_value=1, max_value=120, value=15, step=1, key="sales_forecast_periods")

    if st.button("Run Sales and Units Forecast", key="run_sales_units_forecast_button"):
        st.session_state["run_sales_units_forecast"] = True

    if not st.session_state.get("run_sales_units_forecast"):
        return

    try:
        mode = st.session_state.get("app_mode", "shinny")
        _, sales_forecast = run_sales_forecast(periods=periods, freq=freq, mode=mode)
        _, units_forecast = run_units_forecast(periods=periods, freq=freq, mode=mode)
        _render_forecast_result("Sales Forecast", sales_forecast)
        _render_forecast_result("Units Forecast", units_forecast)
    except Exception as e:
        st.error(f"Error running sales forecast: {e}")


def render_cashflow():
    st.subheader("Cash Flow")
    freq = st.selectbox("Data frequency", options=["daily", "weekly", "monthly"], index=0, key="cashflow_frequency")
    periods = st.number_input("Projection periods", min_value=1, max_value=120, value=15, step=1, key="cashflow_periods")

    if st.button("Run Cash Flow Projection", key="run_cashflow_projection_button"):
        st.session_state["run_cashflow_projection"] = True

    if not st.session_state.get("run_cashflow_projection"):
        return

    try:
        mode = st.session_state.get("app_mode", "shinny")
        _, projection = run_cashflow_projection(periods=periods, freq=freq, mode=mode)

        if projection.empty:
            st.warning("No cash flow projection available.")
            return

        display_df = projection.rename(columns={"ds": "date", "projected_bank_balance": "forecast", "projected_bank_balance_min": "min", "projected_bank_balance_max": "max"})[["date", "forecast", "min", "max"]]
        st.markdown("### Cash Flow Projection Table")
        render_pretty_table(display_df)
        render_cashflow_debug_table(projection)
        st.markdown("### Cash Flow Projection Chart")
        render_forecast_band_chart(display_df, title="Cash Flow Projection")
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
    nav_options = [("Update Data", "update_data", mode == "shinny"), ("Sales Forecast", "sales_forecast", True), ("Cash Flow", "cash_flow", True), ("ROAS", "roas", True)]
    for col, (label, option, enabled) in zip(st.columns(4), nav_options):
        with col:
            if enabled and st.button(label, use_container_width=True):
                st.session_state.selected_option = option

    st.markdown("---")
    if st.session_state.get("selected_option") is None:
        st.info("Choose an option to continue.")


def route_page():
    routes = {
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
