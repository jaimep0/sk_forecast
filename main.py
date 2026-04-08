import pandas as pd
import streamlit as st
from datetime import date

from database import Base, engine

from services.amazon_upload_service import upload_amazon_files_to_db

from services.units_service import (
    prepare_units_dataframe,
    upsert_units_from_dataframe,
)
from services.sales_service import (
    prepare_sales_dataframe,
    upsert_sales_from_dataframe,
)
from services.expenses_service import (
    prepare_expenses_dataframe,
    upsert_expenses_from_dataframe,
)
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

from services.roas_service import get_last_6_weeks_roas

COLUMN_LABELS = {
    "date": "Date",
    "forecast": "Forecast",
    "min": "Min",
    "max": "Max",
    "sales_total": "Sales",
    "units_total": "Units",
    "expenses_total": "Expenses",
    "acquisition_expense_total": "Acquisition Expense",
    "roas": "ROAS",
    "sales_forecast": "Sales Forecast",
    "sales_min": "Sales Min",
    "sales_max": "Sales Max",
    "expenses": "Expenses",
    "net_income": "Net Income",
    "net_income_min": "Net Income Min",
    "net_income_max": "Net Income Max",
    "balance": "Balance",
    "balance_min": "Balance Min",
    "balance_max": "Balance Max",
    "projected_sales": "Sales Forecast",
    "projected_sales_min": "Sales Min",
    "projected_sales_max": "Sales Max",
    "projected_expenses": "Expenses",
    "projected_net_income": "Net Income",
    "projected_net_income_min": "Net Income Min",
    "projected_net_income_max": "Net Income Max",
    "projected_bank_balance": "Balance",
    "projected_bank_balance_min": "Balance Min",
    "projected_bank_balance_max": "Balance Max",
}

st.set_page_config(page_title="Forecast Dashboard", layout="wide")

Base.metadata.create_all(bind=engine)


def render_upload_section(
    title: str,
    uploader_key: str,
    prepare_func,
    upsert_func,
    button_label: str,
):
    st.subheader(title)

    uploaded_file = st.file_uploader(
        f"Upload {title} CSV",
        type=["csv"],
        key=uploader_key,
    )

    if uploaded_file is not None:
        try:
            df = prepare_func(uploaded_file)
            st.write("Preview of uploaded data:")
            st.dataframe(df, use_container_width=True)

            if st.button(button_label, key=f"{uploader_key}_button"):
                inserted, updated = upsert_func(df)
                st.success(
                    f"Upload completed successfully. Inserted: {inserted} rows. Updated: {updated} rows."
                )

        except Exception as e:
            st.error(f"Error processing file: {e}")



def render_forecast_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    min_col: str,
    max_col: str,
):
    if df.empty:
        st.warning("No chart data available.")
        return

    chart_df = df[[x_col, y_col, min_col, max_col]].copy()
    chart_df = chart_df.rename(
        columns={
            x_col: "date",
            y_col: "forecast",
            min_col: "min",
            max_col: "max",
        }
    )
    chart_df["date"] = pd.to_datetime(chart_df["date"])
    chart_df = chart_df.set_index("date")

    st.line_chart(chart_df, use_container_width=True)


import plotly.graph_objects as go


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
            fillcolor="rgba(100, 149, 237, 0.18)",
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
            line=dict(width=3),
        )
    )

    fig.update_layout(
        title=title,
        template="plotly_dark",
        height=420,
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis_title="",
        yaxis_title="",
        legend_title="",
    )

    st.plotly_chart(fig, use_container_width=True)


def render_cashflow_debug_table(cashflow_projection: pd.DataFrame):
    if cashflow_projection.empty:
        return

    debug_df = cashflow_projection.rename(
        columns={
            "ds": "date",
            "projected_sales": "sales_forecast",
            "projected_sales_min": "sales_min",
            "projected_sales_max": "sales_max",
            "projected_expenses": "expenses",
            "projected_net_income": "net_income",
            "projected_net_income_min": "net_income_min",
            "projected_net_income_max": "net_income_max",
            "projected_bank_balance": "balance",
            "projected_bank_balance_min": "balance_min",
            "projected_bank_balance_max": "balance_max",
        }
    )[
        [
            "date",
            "sales_forecast",
            "sales_min",
            "sales_max",
            "expenses",
            "net_income",
            "net_income_min",
            "net_income_max",
            "balance",
            "balance_min",
            "balance_max",
        ]
    ].copy()

    with st.expander("Show detailed cash flow debug table"):
        render_pretty_table(debug_df)


def render_ml_update_section(section_key: str):
    st.markdown("### Update Database")
    st.caption("Only updates Mercado Libre data for marketplace: ml. CSV upload remains available for all other sources.")

    weeks = st.slider(
        "Weeks to update",
        min_value=1,
        max_value=52,
        value=4,
        step=1,
        key=f"{section_key}_weeks_slider",
    )

    if st.button("Update Mercado Libre Data", key=f"{section_key}_ml_update_button"):
        try:
            summary = update_ml_last_weeks(weeks)

            st.success("Mercado Libre update completed successfully.")
            st.write(f"Date range updated: **{summary['start_date']}** to **{summary['end_date']}**")
            st.write(f"Units rows processed: **{summary['units_rows']}**")
            st.write(f"Units inserted: **{summary['units_inserted']}**, updated: **{summary['units_updated']}**")
            st.write(f"Sales rows processed: **{summary['sales_rows']}**")
            st.write(f"Sales inserted: **{summary['sales_inserted']}**, updated: **{summary['sales_updated']}**")

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

    upload_option = st.radio(
        "Choose what you want to update",
        options=["Units", "Sales", "Expenses", "Banks", "Debit"],
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

    elif upload_option == "Debit":
        render_upload_section(
            title="Debit",
            uploader_key="update_debit_csv_uploader",
            prepare_func=prepare_debit_dataframe,
            upsert_func=upsert_debit_from_dataframe,
            button_label="Load Debit into DB",
        )


def render_sales_forecast():
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
            _, sales_forecast = run_sales_forecast(periods=periods, freq=freq)
            _, units_forecast = run_units_forecast(periods=periods, freq=freq)

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
                    }
                )[["date", "forecast", "min", "max"]]
                render_pretty_table(sales_display[["date", "forecast", "min", "max"]])

                st.markdown("### Sales Forecast Chart")
                render_forecast_band_chart(
                    sales_display,
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
                    }
                )[["date", "forecast", "min", "max"]]
                render_pretty_table(units_display[["date", "forecast", "min", "max"]])

                st.markdown("### Units Forecast Chart")
                render_forecast_band_chart(
                    units_display,
                    title="Units Forecast",
                )

        except Exception as e:
            st.error(f"Error running sales forecast: {e}")


def render_cashflow():
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
            _, cashflow_projection = run_cashflow_projection(periods=periods, freq=freq)

            if cashflow_projection.empty:
                st.warning("No cash flow projection available.")
            else:
                cashflow_display = cashflow_projection.rename(
                    columns={
                        "ds": "date",
                        "projected_bank_balance": "forecast",
                        "projected_bank_balance_min": "min",
                        "projected_bank_balance_max": "max",
                    }
                )[["date", "forecast", "min", "max"]]

                st.markdown("### Cash Flow Projection Table")
                render_pretty_table(cashflow_display[["date", "forecast", "min", "max"]])

                render_cashflow_debug_table(cashflow_projection)

                st.markdown("### Cash Flow Projection Chart")
                render_forecast_band_chart(
                    cashflow_display,
                    title="Cash Flow Projection",
                )

        except Exception as e:
            st.error(f"Error running cash flow projection: {e}")


def render_roas():
    st.subheader("ROAS")

    latest_date = get_latest_acquisition_expense_date()
    if latest_date is None:
        st.info("No acquisition expense data saved yet.")
    else:
        st.write(f"Latest date with data: **{latest_date}**")

    if "roas_use_manual_date" not in st.session_state:
        st.session_state.roas_use_manual_date = False

    if st.button("Update data", key="roas_update_data_button"):
        st.session_state.roas_show_update = True

    if st.session_state.get("roas_show_update"):
        missing_dates = get_missing_week_end_dates_since_latest()

        options = ["Manual date"] + [d.isoformat() for d in missing_dates]

        selected_option = st.selectbox(
            "Select week date to update",
            options=options,
            key="roas_date_selector",
        )

        if selected_option == "Manual date":
            selected_date = st.date_input(
                "Choose any date",
                value=date.today(),
                key="roas_manual_date_input",
            )
        else:
            selected_date = pd.to_datetime(selected_option).date()

        st.markdown("### Acquisition expense inputs")

        amazon = st.number_input("Amazon", min_value=0.0, value=0.0, step=100.0, key="roas_amazon")
        mercado_libre = st.number_input("Mercado Libre", min_value=0.0, value=0.0, step=100.0, key="roas_ml")
        facebook = st.number_input("Facebook", min_value=0.0, value=0.0, step=100.0, key="roas_facebook")
        tiktok = st.number_input("Tiktok", min_value=0.0, value=0.0, step=100.0, key="roas_tiktok")
        google = st.number_input("Google", min_value=0.0, value=0.0, step=100.0, key="roas_google")
        ugc_colab = st.number_input("UGC_y_Colab", min_value=0.0, value=0.0, step=100.0, key="roas_ugc")
        otros = st.number_input("Otros", min_value=0.0, value=0.0, step=100.0, key="roas_otros")

        if st.button("Save acquisition expense", key="roas_save_button"):
            inserted, updated = upsert_acquisition_expense_row(
                row_date=selected_date,
                Amazon=amazon,
                Mercado_Libre=mercado_libre,
                Facebook=facebook,
                Tiktok=tiktok,
                Google=google,
                UGC_y_Colab=ugc_colab,
                Otros=otros,
            )
            st.success(f"Saved successfully. Inserted: {inserted}, Updated: {updated}")

    st.markdown("### ROAS - Last 6 weeks")

    roas_df = get_last_6_weeks_roas()

    if roas_df.empty:
        st.warning("No ROAS data available yet.")
    else:
        roas_display = roas_df.copy()
        roas_display["date"] = pd.to_datetime(roas_display["date"])

        render_pretty_table(
            roas_display[["date", "sales_total", "acquisition_expense_total", "roas"]],
            percent_cols=["roas"],
        )

        chart_df = roas_display[["date", "roas"]].copy().set_index("date")
        st.line_chart(chart_df, use_container_width=True)


def render_banks_update_section():
    st.markdown("### Banks Weekly Update")

    latest_date = get_latest_banks_date()
    if latest_date is None:
        st.info("No bank data saved yet.")
    else:
        st.write(f"Latest date with data: **{latest_date}**")

    if st.button("Update banks data", key="banks_update_data_button"):
        st.session_state.banks_show_update = True

    if st.session_state.get("banks_show_update"):
        missing_dates = get_missing_bank_week_end_dates_since_latest()
        options = ["Manual date"] + [d.isoformat() for d in missing_dates]

        selected_option = st.selectbox(
            "Select bank week date to update",
            options=options,
            key="banks_date_selector",
        )

        if selected_option == "Manual date":
            selected_date = st.date_input(
                "Choose any bank date",
                value=date.today(),
                key="banks_manual_date_input",
            )
        else:
            selected_date = pd.to_datetime(selected_option).date()

        bbva = st.number_input("BBVA", value=0.0, step=1000.0, key="banks_bbva")
        brg = st.number_input("BRG", value=0.0, step=1000.0, key="banks_brg")
        mp = st.number_input("MP", value=0.0, step=1000.0, key="banks_mp")
        mp_liberar = st.number_input("MP Liberar", value=0.0, step=1000.0, key="banks_mp_liberar")
        shop = st.number_input("Shop", value=0.0, step=1000.0, key="banks_shop")
        lvp = st.number_input("LVP", value=0.0, step=1000.0, key="banks_lvp")
        coppel = st.number_input("Amazon", value=0.0, step=1000.0, key="banks_coppel")

        if st.button("Save banks data", key="banks_save_button"):
            inserted, updated = upsert_banks_row(
                row_date=selected_date,
                bbva=bbva,
                brg=brg,
                mp=mp,
                mp_liberar=mp_liberar,
                shop=shop,
                lvp=lvp,
                coppel=coppel,
            )
            st.success(f"Saved successfully. Inserted: {inserted}, Updated: {updated}")


def render_amazon_upload_section():
    st.markdown("### Amazon TXT Upload")
    st.caption("Upload one or more Amazon order report .txt files. This updates sales and units for marketplace: amzn.")

    uploaded_files = st.file_uploader(
        "Upload Amazon TXT files",
        type=["txt"],
        accept_multiple_files=True,
        key="amazon_txt_uploader",
    )

    if uploaded_files:
        if st.button("Load Amazon files into DB", key="load_amazon_txt_button"):
            try:
                summary = upload_amazon_files_to_db(uploaded_files)

                st.success("Amazon data uploaded successfully.")
                st.write(f"Units rows processed: **{summary['units_rows']}**")
                st.write(f"Units inserted: **{summary['units_inserted']}**, updated: **{summary['units_updated']}**")
                st.write(f"Sales rows processed: **{summary['sales_rows']}**")
                st.write(f"Sales inserted: **{summary['sales_inserted']}**, updated: **{summary['sales_updated']}**")

                st.markdown("#### Units preview")
                st.dataframe(summary["df_units"], use_container_width=True)

                st.markdown("#### Sales preview")
                st.dataframe(summary["df_sales"], use_container_width=True)

            except Exception as e:
                st.error(f"Error uploading Amazon files: {e}")


def render_home():
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
            max-width: 1200px;
        }
        div.stButton > button {
            width: 100%;
            border-radius: 10px;
            height: 48px;
            font-weight: 600;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("Forecast Dashboard")
    st.caption("Update your operational data and monitor forecasts, cash flow, and ROAS.")

    if "selected_option" not in st.session_state:
        st.session_state.selected_option = None

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("Update Data"):
            st.session_state.selected_option = "update_data"

    with col2:
        if st.button("Sales Forecast"):
            st.session_state.selected_option = "sales_forecast"

    with col3:
        if st.button("Cash Flow"):
            st.session_state.selected_option = "cash_flow"

    with col4:
        if st.button("ROAS"):
            st.session_state.selected_option = "roas"

    st.markdown("---")

    if st.session_state.selected_option is None:
        st.info("Choose a module to continue.")
        

def route_page():
    option = st.session_state.get("selected_option")

    if option == "upload_units":
        render_upload_section(
            title="Units",
            uploader_key="units_csv_uploader",
            prepare_func=prepare_units_dataframe,
            upsert_func=upsert_units_from_dataframe,
            button_label="Load Units into DB",
        )

    elif option == "upload_sales":
        render_upload_section(
            title="Sales",
            uploader_key="sales_csv_uploader",
            prepare_func=prepare_sales_dataframe,
            upsert_func=upsert_sales_from_dataframe,
            button_label="Load Sales into DB",
        )

    elif option == "upload_expenses":
        render_upload_section(
            title="Expenses",
            uploader_key="expenses_csv_uploader",
            prepare_func=prepare_expenses_dataframe,
            upsert_func=upsert_expenses_from_dataframe,
            button_label="Load Expenses into DB",
        )

    elif option == "upload_banks":
        render_upload_section(
            title="Banks",
            uploader_key="banks_csv_uploader",
            prepare_func=prepare_banks_dataframe,
            upsert_func=upsert_banks_from_dataframe,
            button_label="Load Banks into DB",
        )

    elif option == "sales_forecast":
        render_sales_forecast()

    elif option == "cash_flow":
        render_cashflow()

    elif option == "update_data":
        render_update_data()
    
    elif option == "roas":
        render_roas()

    else:
        st.info("Choose an option to continue.")


def render_pretty_table(
    df: pd.DataFrame,
    date_col: str = "date",
    percent_cols: list[str] | None = None,
    decimals: int = 2,
):
    if df.empty:
        st.warning("No data available.")
        return

    percent_cols = percent_cols or []
    out = df.copy()

    if date_col in out.columns:
        out[date_col] = pd.to_datetime(out[date_col]).dt.strftime("%Y-%m-%d")
        out = out.set_index(date_col)

    out = out.rename(columns=COLUMN_LABELS)

    format_dict = {}
    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]):
            if col in [COLUMN_LABELS.get(c, c) for c in percent_cols]:
                format_dict[col] = "{:.4f}"
            else:
                format_dict[col] = f"{{:,.{decimals}f}}"

    st.dataframe(out.style.format(format_dict), use_container_width=True)


def main():
    render_home()
    route_page()


if __name__ == "__main__":
    main()