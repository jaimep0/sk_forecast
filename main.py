import pandas as pd
import streamlit as st

from tools import *

def main():
    st.markdown("""
    <style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1100px;
    }
    </style>
    """, unsafe_allow_html=True)

    st.set_page_config(page_title="Forecast Dashboard", layout="wide")

    st.title("Forecast Dashboard")
    st.caption("Load your data and get forecasts for the next 15 periods.")

main()  


if "selected_option" not in st.session_state:
    st.session_state.selected_option = None

col1, col2 = st.columns(2)

with col1:
    if st.button("Load Data to forecast Sales"):
        st.session_state.selected_option = "Sales"

with col2:
    if st.button("Load Sales and Expenses Data ($)"):
        st.session_state.selected_option = "Sales and Expenses"

if st.session_state.selected_option == "Sales":
    ForecastDashboard.data_forecast("Sales")
elif st.session_state.selected_option == "Sales and Expenses":
    cashflow.cashflow("Cash flow")
else:
    st.info("Click a button to forecast data.")
#ForecastDashboard.data_forecast('Expenses')