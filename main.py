import pandas as pd
import streamlit as st

from database import Base, engine

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
)
from services.debit_service import (
    prepare_debit_dataframe,
    upsert_debit_from_dataframe,
)

from services.forecast_run_service import (
    run_sales_forecast,
    run_units_forecast,
    run_cashflow_projection,
)


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
    title: str,
    x_col: str,
    y_col: str,
    min_col: str,
    max_col: str,
):
    if df.empty:
        st.warning(f"No data available for {title}.")
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


def render_sales_forecast():
    st.subheader("Sales Forecast")

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
            _, sales_forecast = run_sales_forecast(periods=periods)
            _, units_forecast = run_units_forecast(periods=periods)

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
                st.dataframe(sales_display, use_container_width=True)

                st.markdown("### Sales Forecast Chart")
                render_forecast_chart(
                    df=sales_display,
                    title="Sales Forecast",
                    x_col="date",
                    y_col="forecast",
                    min_col="min",
                    max_col="max",
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
                st.dataframe(units_display, use_container_width=True)

                st.markdown("### Units Forecast Chart")
                render_forecast_chart(
                    df=units_display,
                    title="Units Forecast",
                    x_col="date",
                    y_col="forecast",
                    min_col="min",
                    max_col="max",
                )

        except Exception as e:
            st.error(f"Error running sales forecast: {e}")


def render_cashflow():
    st.subheader("Cash Flow")

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
            _, cashflow_projection = run_cashflow_projection(periods=periods)

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
                st.dataframe(cashflow_display, use_container_width=True)

                st.markdown("### Cash Flow Projection Chart")
                render_forecast_chart(
                    df=cashflow_display,
                    title="Cash Flow Projection",
                    x_col="date",
                    y_col="forecast",
                    min_col="min",
                    max_col="max",
                )

        except Exception as e:
            st.error(f"Error running cash flow projection: {e}")


def render_home():
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
            max-width: 1200px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("Forecast Dashboard")
    st.caption("Upload database tables and run forecasts.")

    if "selected_option" not in st.session_state:
        st.session_state.selected_option = None

    col1, col2, col3 = st.columns(3)
    col4, col5, col6 = st.columns(3)
    col7, col8 = st.columns(2)

    with col1:
        if st.button("Upload Units"):
            st.session_state.selected_option = "upload_units"

    with col2:
        if st.button("Upload Sales"):
            st.session_state.selected_option = "upload_sales"

    with col3:
        if st.button("Upload Expenses"):
            st.session_state.selected_option = "upload_expenses"

    with col4:
        if st.button("Upload Banks"):
            st.session_state.selected_option = "upload_banks"

    with col5:
        if st.button("Upload Debit"):
            st.session_state.selected_option = "upload_debit"

    with col6:
        if st.button("Sales Forecast"):
            st.session_state.selected_option = "sales_forecast"

    with col7:
        if st.button("Cash Flow"):
            st.session_state.selected_option = "cash_flow"

    with col8:
        if st.button("Home"):
            st.session_state.selected_option = None


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

    elif option == "upload_debit":
        render_upload_section(
            title="Debit",
            uploader_key="debit_csv_uploader",
            prepare_func=prepare_debit_dataframe,
            upsert_func=upsert_debit_from_dataframe,
            button_label="Load Debit into DB",
        )

    elif option == "sales_forecast":
        render_sales_forecast()

    elif option == "cash_flow":
        render_cashflow()

    else:
        st.info("Choose an option to continue.")


def main():
    render_home()
    route_page()


if __name__ == "__main__":
    main()