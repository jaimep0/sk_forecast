import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from datetime import date, timedelta
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
from services.shopify_update_service import update_shopify_last_weeks
from services.acquisition_expense_service import (
    upsert_acquisition_expense_row,
    get_latest_acquisition_expense_date,
    get_missing_week_end_dates_since_latest,
)
from services.roas_service import get_last_6_weeks_roas_by_mode
from services.test_data_service import (
    get_test_expenses_daily_totals,
    get_test_banks_daily_totals,
)

st.set_page_config(page_title="ShinnySkin Dashboard", page_icon="✨", layout="wide")
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
        :root {
            --ss-pink: #ec5b8d;
            --ss-pink-soft: #f7dce5;
            --ss-pink-lighter: #fff3f7;
            --ss-rose: #d97998;
            --ss-black: #111111;
            --ss-muted: #6f6470;
            --ss-border: rgba(236, 91, 141, 0.20);
            --ss-card: rgba(255,255,255,0.84);
        }
        .stApp {
            background: radial-gradient(circle at top left, rgba(236,91,141,0.24), transparent 32%),
                        linear-gradient(180deg, #fff7fa 0%, #ffffff 48%, #fff3f7 100%);
            color: var(--ss-black);
        }
        .block-container {padding-top: 1.4rem; padding-bottom: 2.2rem; max-width: 1320px;}
        h1, h2, h3 {color: var(--ss-black); letter-spacing: -0.035em;}
        .ss-hero {
            position: relative; overflow: hidden; padding: 1.45rem 1.75rem; margin-bottom: 1.2rem;
            border-radius: 26px;
            background: linear-gradient(135deg, rgba(255,255,255,0.98) 0%, rgba(255,244,248,0.96) 58%, rgba(248,215,226,0.92) 100%);
            border: 1px solid rgba(236,91,141,0.18);
            box-shadow: 0 22px 54px rgba(236,91,141,0.13);
        }
        .ss-hero::after {content: "✦"; position: absolute; right: 30px; top: 18px; font-size: 3rem; color: rgba(17,17,17,0.08);}
        .ss-brand-logo {
            color: #111111; font-size: 2.15rem; line-height: 1; font-weight: 500;
            letter-spacing: .18em; font-family: Arial, Helvetica, sans-serif;
            margin-bottom: 1rem; text-transform: uppercase;
        }
        .ss-eyebrow {color: var(--ss-pink); font-size: .74rem; font-weight: 800; letter-spacing: .18em; text-transform: uppercase; margin-bottom: .28rem;}
        .ss-hero h1 {color: var(--ss-black); margin: 0 0 .25rem 0; font-weight: 800; font-size: 2.05rem;}
        .ss-hero p {color: #4d4248; max-width: 840px; margin: 0; font-size: .98rem;}
        .metric-card {
            padding: 1rem 1.05rem; border-radius: 22px; background: var(--ss-card);
            border: 1px solid var(--ss-border); box-shadow: 0 16px 42px rgba(236,91,141,0.12); min-height: 112px;
        }
        .metric-label {color: var(--ss-rose); font-size: .76rem; text-transform: uppercase; letter-spacing: .09em; font-weight: 800;}
        .metric-value {color: var(--ss-black); font-size: 1.7rem; font-weight: 850; line-height: 1.1; margin-top: .18rem;}
        .metric-sub {color: var(--ss-muted); font-size: .82rem; margin-top: .35rem;}
        .section-card, .insight-box {
            background: rgba(255,255,255,0.74); border: 1px solid var(--ss-border); border-radius: 24px;
            padding: 1.1rem 1.2rem; box-shadow: 0 16px 42px rgba(236,91,141,0.10);
        }
        [data-testid="stMetric"] {
            background: rgba(255,255,255,0.84); border: 1px solid var(--ss-border); border-radius: 22px;
            padding: 1rem; box-shadow: 0 16px 42px rgba(236,91,141,0.10);
        }
        [data-testid="stMetricLabel"] p {color: var(--ss-rose); font-weight: 800;}
        [data-testid="stMetricValue"] {color: var(--ss-black);}
        div.stButton > button {
            width: 100%; border-radius: 16px; min-height: 46px; font-weight: 800;
            border: 1px solid rgba(236,91,141,0.32);
            background: linear-gradient(135deg, #ffffff 0%, #fff3f7 100%); color: var(--ss-black);
            box-shadow: 0 10px 26px rgba(236,91,141,0.10);
        }
        div.stButton > button:hover {border-color: var(--ss-pink); color: var(--ss-pink); box-shadow: 0 14px 30px rgba(236,91,141,0.18);}
        div[data-testid="stDataFrame"] {border-radius: 18px; overflow: hidden; border: 1px solid var(--ss-border);}
        [data-testid="stPlotlyChart"] {
            background: rgba(255,255,255,0.86); border: 1px solid rgba(236,91,141,0.16);
            border-radius: 24px; padding: .55rem; box-shadow: 0 16px 42px rgba(236,91,141,0.08);
        }
        label, .stNumberInput label, .stSelectbox label, .stRadio label {color: #111111 !important; font-weight: 750 !important;}

        /* Friendly hover navigation rail */
        section[data-testid="stSidebar"] {
            width: 82px !important;
            min-width: 82px !important;
            transition: width 220ms ease, min-width 220ms ease;
            border-right: 1px solid rgba(236,91,141,0.18);
            box-shadow: 12px 0 34px rgba(236,91,141,0.08);
        }
        section[data-testid="stSidebar"]:hover {
            width: 290px !important;
            min-width: 290px !important;
        }
        section[data-testid="stSidebar"] > div {
            background: linear-gradient(180deg, rgba(255,255,255,0.98) 0%, rgba(255,243,247,0.98) 100%);
            padding-top: 1.1rem;
        }
        section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {
            padding: 1rem .85rem;
        }
        .ss-sidebar-rail {
            display: flex; align-items: center; gap: .65rem;
            padding: .8rem .55rem 1rem .55rem; margin-bottom: .45rem;
            border-bottom: 1px solid rgba(236,91,141,0.14);
        }
        .ss-sidebar-logo-mark {
            width: 42px; height: 42px; border-radius: 15px;
            display: flex; align-items: center; justify-content: center;
            color: #111111; background: #ffffff;
            border: 1px solid rgba(236,91,141,0.20);
            box-shadow: 0 10px 24px rgba(236,91,141,0.12);
            font-weight: 900; letter-spacing: .05em; flex: 0 0 42px;
        }
        .ss-sidebar-copy {
            opacity: 0; transform: translateX(-8px); white-space: nowrap; overflow: hidden;
            transition: opacity 180ms ease, transform 180ms ease;
        }
        section[data-testid="stSidebar"]:hover .ss-sidebar-copy {
            opacity: 1; transform: translateX(0);
        }
        .ss-sidebar-brand {
            color: #111111; font-size: 1.02rem; font-weight: 650;
            letter-spacing: .18em; line-height: 1;
        }
        .ss-sidebar-mode {color: #d97998; font-size: .72rem; font-weight: 800; margin-top: .24rem;}
        .ss-sidebar-hint {
            color: #8a7a82; font-size: .74rem; padding: .35rem .7rem .8rem .7rem;
            opacity: 1;
        }
        section[data-testid="stSidebar"]:hover .ss-sidebar-hint {opacity: .9;}
        section[data-testid="stSidebar"] div.stButton > button {
            min-height: 44px; border-radius: 16px; justify-content: flex-start; padding-left: .75rem;
            background: rgba(255,255,255,0.82); color: #111111;
        }
        section[data-testid="stSidebar"] div.stButton > button p {
            white-space: nowrap; overflow: hidden; text-overflow: clip;
        }
        section[data-testid="stSidebar"]:not(:hover) div.stButton > button {
            font-size: 0; padding-left: 0; padding-right: 0; justify-content: center;
        }
        section[data-testid="stSidebar"]:not(:hover) div.stButton > button p {
            font-size: 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_metric_card(label: str, value: str, note: str = ""):
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-sub">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_hero(title: str, subtitle: str, eyebrow: str = "ShinnySkin"):
    st.markdown(
        f"""
        <div class="ss-hero">
            <div class="ss-brand-logo">SHINNYSKIN</div>
            <div class="ss-eyebrow">{eyebrow}</div>
            <h1>{title}</h1>
            <p>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def apply_shinny_plot_layout(fig: go.Figure, title: str = "", height: int = 400, yaxis_title: str = "") -> go.Figure:
    fig.update_layout(
        title=dict(text=title, font=dict(size=20, color="#111111", family="Arial"), x=0.02),
        template="plotly_white",
        height=height,
        margin=dict(l=48, r=28, t=64, b=58),
        paper_bgcolor="rgba(255,255,255,0.96)",
        plot_bgcolor="#FFFFFF",
        font=dict(color="#111111", size=13, family="Arial"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(color="#111111", size=12),
            bgcolor="rgba(255,255,255,0.72)",
        ),
        hoverlabel=dict(bgcolor="#111111", font_size=12, font_color="#FFFFFF"),
        xaxis_title="",
        yaxis_title=yaxis_title,
        legend_title="",
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="rgba(17,17,17,0.08)",
        zeroline=False,
        linecolor="rgba(17,17,17,0.25)",
        tickfont=dict(color="#111111", size=12),
        title_font=dict(color="#111111", size=13),
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(17,17,17,0.08)",
        zeroline=False,
        linecolor="rgba(17,17,17,0.25)",
        tickfont=dict(color="#111111", size=12),
        title_font=dict(color="#111111", size=13),
    )
    return fig


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
            fillcolor="rgba(236,91,141,0.18)",
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
            line=dict(color="#EC5B8D", width=3),
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
                    marker=dict(size=9, color="#111111", line=dict(color="#EC5B8D", width=2)),
                )
            )

    apply_shinny_plot_layout(fig, title=title, height=420)

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


def render_shopify_update_section(section_key: str):
    st.markdown("### Shopify Update Database")
    st.caption("Only updates Shopify data for marketplace: shopify. CSV upload remains available for all other sources.")

    weeks = st.slider(
        "Weeks to update",
        min_value=1,
        max_value=52,
        value=4,
        step=1,
        key=f"{section_key}_weeks_slider",
    )

    if st.button("Update Shopify Data", key=f"{section_key}_shopify_update_button"):
        try:
            summary = update_shopify_last_weeks(weeks)
            st.success("Shopify update completed successfully.")
            st.write(f"Date range updated: **{summary['start_date']}** to **{summary['end_date']}**")

            for table in ["units", "sales"]:
                st.write(f"{table.title()} rows processed: **{summary[f'{table}_rows']}**")
                st.write(
                    f"{table.title()} inserted: **{summary[f'{table}_inserted']}**, "
                    f"updated: **{summary[f'{table}_updated']}**"
                )

        except Exception as e:
            st.error(f"Error updating Shopify data: {e}")


def render_update_data():
    inject_dashboard_theme()
    st.subheader("Update Data")

    st.markdown("## Mercado Libre")
    render_ml_update_section("update_data_ml")

    st.markdown("---")
    st.markdown("## Shopify")
    render_shopify_update_section("update_data_shopify")

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
    sales_hist = sales_history.rename(columns={"ds": "date"}).copy() if sales_history is not None else pd.DataFrame()
    cash_hist = cashflow_history.copy() if cashflow_history is not None else pd.DataFrame()

    last_week_sales = 0.0
    if not sales_hist.empty and {"date", "y"}.issubset(sales_hist.columns):
        sales_hist["date"] = pd.to_datetime(sales_hist["date"])
        sales_hist["y"] = pd.to_numeric(sales_hist["y"], errors="coerce").fillna(0)
        last_sales_date = sales_hist["date"].max()
        last_sales_start = last_sales_date - pd.Timedelta(days=6)
        last_week_sales = float(sales_hist.loc[(sales_hist["date"] >= last_sales_start) & (sales_hist["date"] <= last_sales_date), "y"].sum())

    last_cashflow_row = _latest_completed_row(cash_hist)
    next_week_sales_forecast = _next_forecast_value(sales_display)
    last_week_balance = float(last_cashflow_row.get("banks_total", 0) or 0) if last_cashflow_row is not None else 0
    next_projected_balance = _next_forecast_value(cashflow_display)
    future_expenses_total = float(upcoming_expenses["total"].sum()) if not upcoming_expenses.empty else 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_metric_card("Last 7 days sales", f"${last_week_sales:,.0f}", "Latest 7 saved days")
    with col2:
        render_metric_card("Next forecasted span sales", f"${next_week_sales_forecast:,.0f}", "First forecasted period")
    with col3:
        render_metric_card("Latest real balance", f"${last_week_balance:,.0f}", "Latest saved bank balance")
    with col4:
        render_metric_card("Next projected balance", f"${next_projected_balance:,.0f}", f"Upcoming expenses: ${future_expenses_total:,.0f}")


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
            marker_color="#EC5B8D",
            text=df[y_col],
            texttemplate=f"{value_prefix}%{{text:,.0f}}",
            textposition="outside",
            textfont=dict(color="#111111", size=12),
        )
    )
    apply_shinny_plot_layout(fig, title=title, height=390)
    fig.update_layout(showlegend=False, uniformtext_minsize=10, uniformtext_mode="hide")
    fig.update_xaxes(tickangle=-25)
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
        render_metric_card("Latest ROAS", f"{latest['roas']:.2f}x", "Last saved week")
    with col2:
        render_metric_card("Latest sales", f"${float(latest.get('sales_total', 0) or 0):,.0f}", "ROAS sales base")
    with col3:
        render_metric_card("Latest acquisition expense", f"${float(latest.get('acquisition_expense_total', 0) or 0):,.0f}", "Marketing investment")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=chart_df["date"],
            y=chart_df["roas"],
            mode="lines+markers+text",
            text=chart_df["roas"].round(2),
            textposition="top center",
            name="ROAS",
            line=dict(color="#EC5B8D", width=3),
            marker=dict(size=8),
        )
    )
    fig.update_traces(textfont=dict(color="#111111", size=12))
    apply_shinny_plot_layout(fig, title="ROAS - Last 6 Saved Weeks", height=390, yaxis_title="ROAS")
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

def _time_span_days(group_by: str, custom_days: int) -> int:
    if group_by == "Day":
        return 1
    if group_by == "Week":
        return 7
    if group_by == "Month":
        return 31
    return max(int(custom_days or 1), 1)


def _default_summary_start_date(last_date: pd.Timestamp | None, group_by: str, custom_days: int, past_spans: int = 4) -> date:
    base = pd.Timestamp(date.today()) if last_date is None or pd.isna(last_date) else pd.to_datetime(last_date)
    days = _time_span_days(group_by, custom_days) * past_spans
    return (base - pd.Timedelta(days=days - 1)).date()


def _build_group_key(df: pd.DataFrame, date_col: str, group_by: str, custom_days: int, anchor_date: pd.Timestamp) -> pd.Series:
    dates = pd.to_datetime(df[date_col])
    if group_by == "Day":
        return dates.dt.floor("D")
    if group_by == "Week":
        return dates.dt.to_period("W-SUN").dt.start_time
    if group_by == "Month":
        return dates.dt.to_period("M").dt.start_time

    days = max(int(custom_days or 1), 1)
    anchor = pd.to_datetime(anchor_date).normalize()
    bucket_number = ((dates.dt.normalize() - anchor).dt.days // days).clip(lower=0)
    return anchor + pd.to_timedelta(bucket_number * days, unit="D")


def _aggregate_forecast_display(
    display_df: pd.DataFrame,
    group_by: str,
    custom_days: int,
    start_date: date,
    forecasted_periods: int,
    aggregation: str,
) -> pd.DataFrame:
    empty = pd.DataFrame(columns=["date", "forecast", "min", "max", "real"])
    if display_df is None or display_df.empty:
        return empty

    out = display_df.copy()
    out["date"] = pd.to_datetime(out["date"])
    for col in ["forecast", "min", "max", "real"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    start_ts = pd.Timestamp(start_date).normalize()
    real_dates = out.loc[out["real"].notna(), "date"]
    last_real_date = real_dates.max() if not real_dates.empty else out["date"].min()

    past = out[(out["date"] >= start_ts) & (out["date"] <= last_real_date)].copy()
    future = out[out["date"] > last_real_date].copy()

    def aggregate(part: pd.DataFrame, keep_first_n: int | None = None) -> pd.DataFrame:
        if part.empty:
            return empty.copy()
        part = part.copy()
        part["period_start"] = _build_group_key(part, "date", group_by, custom_days, start_ts)
        if aggregation == "last":
            grouped = (
                part.sort_values("date")
                .groupby("period_start", as_index=False)
                .agg({"forecast": "last", "min": "last", "max": "last", "real": "last"})
                .rename(columns={"period_start": "date"})
            )
        else:
            grouped = (
                part.groupby("period_start", as_index=False)
                .agg({"forecast": "sum", "min": "sum", "max": "sum", "real": "sum"})
                .rename(columns={"period_start": "date"})
            )
            if part["real"].isna().all():
                grouped["real"] = pd.NA

        grouped = grouped.sort_values("date").reset_index(drop=True)
        if keep_first_n is not None:
            grouped = grouped.head(keep_first_n)
        return grouped[["date", "forecast", "min", "max", "real"]]

    past_grouped = aggregate(past)
    future_grouped = aggregate(future, keep_first_n=forecasted_periods)
    return pd.concat([past_grouped, future_grouped], ignore_index=True)


def _forecast_days_needed(group_by: str, custom_days: int, forecasted_periods: int) -> int:
    return max(_time_span_days(group_by, custom_days) * int(forecasted_periods), int(forecasted_periods))


PRODUCT_HEATMAP_COLORS = [
    "#EC5B8D", "#111111", "#7A4E9E", "#00A8A8", "#F59E0B",
    "#2563EB", "#10B981", "#EF4444", "#8B5CF6", "#14B8A6",
    "#F97316", "#64748B", "#DB2777", "#22C55E", "#06B6D4",
    "#A855F7", "#EAB308", "#0F766E", "#BE123C", "#475569",
]


def _product_color_map() -> dict[str, str]:
    return {
        product: PRODUCT_HEATMAP_COLORS[index % len(PRODUCT_HEATMAP_COLORS)]
        for index, product in enumerate(PRODUCT_COLUMNS)
    }


def _pretty_product_name(product: str) -> str:
    return str(product).replace("_", " ").strip().title()


def _render_sales_heatmap(grouped_sales: pd.DataFrame):
    if grouped_sales.empty:
        st.warning("No sales data available for the selected filters.")
        return

    color_map = _product_color_map()
    market_totals = grouped_sales.groupby("mkp_name", as_index=False)["sales"].sum()

    labels = ["Total Sales"]
    ids = ["total"]
    parents = [""]
    values = [float(market_totals["sales"].sum())]
    colors = ["#FFF3F7"]
    customdata = [["All markets", "All products"]]

    for _, market_row in market_totals.sort_values("sales", ascending=False).iterrows():
        market = str(market_row["mkp_name"]).upper()
        market_id = f"market::{market}"
        labels.append(market)
        ids.append(market_id)
        parents.append("total")
        values.append(float(market_row["sales"]))
        colors.append("#F7DCE5")
        customdata.append([market, "All products"])

        market_products = grouped_sales[grouped_sales["mkp_name"].str.upper() == market].sort_values("sales", ascending=False)
        for _, product_row in market_products.iterrows():
            product = str(product_row["product"])
            product_label = _pretty_product_name(product)
            labels.append(product_label)
            ids.append(f"{market_id}::{product}")
            parents.append(market_id)
            values.append(float(product_row["sales"]))
            colors.append(color_map.get(product, "#EC5B8D"))
            customdata.append([market, product_label])

    fig = go.Figure(
        go.Treemap(
            labels=labels,
            ids=ids,
            parents=parents,
            values=values,
            branchvalues="total",
            marker=dict(colors=colors, line=dict(color="white", width=2)),
            customdata=customdata,
            texttemplate="<b>%{label}</b><br>$%{value:,.0f}",
            hovertemplate=(
                "<b>%{label}</b><br>"
                "Market: %{customdata[0]}<br>"
                "Product: %{customdata[1]}<br>"
                "Sales: $%{value:,.2f}<extra></extra>"
            ),
            tiling=dict(packing="squarify"),
        )
    )
    apply_shinny_plot_layout(fig, title="Sales Heat Map by Market and Product", height=650)
    fig.update_layout(margin=dict(l=10, r=10, t=64, b=10))
    st.plotly_chart(fig, use_container_width=True)


def render_detailed_sales():
    inject_dashboard_theme()
    st.subheader("Detailed Sales")
    st.caption("Heat map grouped by market. Each product keeps its own color across all markets.")

    mode = st.session_state.get("app_mode", "shinny")
    sales_df = _get_sales_history_by_mode(mode)

    if sales_df.empty:
        st.warning("No sales data available.")
        return

    sales_df = sales_df.copy()
    sales_df["date"] = pd.to_datetime(sales_df["date"]).dt.normalize()
    min_date = sales_df["date"].min().date()
    max_date = sales_df["date"].max().date()
    default_start = max(min_date, (pd.Timestamp(max_date) - pd.Timedelta(days=29)).date())

    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        start_date = st.date_input("Start date", value=default_start, min_value=min_date, max_value=max_date, key="detailed_sales_start")
    with col2:
        end_date = st.date_input("End date", value=max_date, min_value=min_date, max_value=max_date, key="detailed_sales_end")
    with col3:
        market_options = sorted(sales_df["mkp_name"].dropna().unique().tolist())
        selected_markets = st.multiselect(
            "Markets",
            options=market_options,
            default=market_options,
            key="detailed_sales_markets",
        )

    if pd.Timestamp(start_date) > pd.Timestamp(end_date):
        st.error("Start date cannot be after end date.")
        return

    filtered = sales_df[
        (sales_df["date"] >= pd.Timestamp(start_date))
        & (sales_df["date"] <= pd.Timestamp(end_date))
    ].copy()
    if selected_markets:
        filtered = filtered[filtered["mkp_name"].isin(selected_markets)].copy()

    melted = filtered.melt(
        id_vars=["date", "mkp_name"],
        value_vars=PRODUCT_COLUMNS,
        var_name="product",
        value_name="sales",
    )
    melted["sales"] = pd.to_numeric(melted["sales"], errors="coerce").fillna(0)
    melted = melted[melted["sales"] > 0].copy()

    if melted.empty:
        st.warning("No positive sales found for this selection.")
        return

    grouped_sales = (
        melted.groupby(["mkp_name", "product"], as_index=False)["sales"]
        .sum()
        .sort_values("sales", ascending=False)
    )

    total_sales = float(grouped_sales["sales"].sum())
    top_market = grouped_sales.groupby("mkp_name")["sales"].sum().sort_values(ascending=False)
    top_product = grouped_sales.groupby("product")["sales"].sum().sort_values(ascending=False)

    metric_col1, metric_col2, metric_col3 = st.columns(3)
    with metric_col1:
        render_metric_card("Total sales", f"${total_sales:,.0f}", f"{start_date} to {end_date}")
    with metric_col2:
        render_metric_card("Top market", str(top_market.index[0]).upper(), f"${float(top_market.iloc[0]):,.0f}")
    with metric_col3:
        render_metric_card("Top product", _pretty_product_name(str(top_product.index[0])), f"${float(top_product.iloc[0]):,.0f}")

    _render_sales_heatmap(grouped_sales)

    st.markdown("### Product color legend")
    legend = (
        grouped_sales.groupby("product", as_index=False)["sales"]
        .sum()
        .sort_values("sales", ascending=False)
    )
    color_map = _product_color_map()
    legend_html = "".join(
        f'''<span style="display:inline-flex;align-items:center;gap:6px;margin:0 10px 10px 0;padding:7px 10px;border-radius:999px;background:white;border:1px solid rgba(236,91,141,.20);">
              <span style="width:12px;height:12px;border-radius:50%;background:{color_map.get(row['product'], '#EC5B8D')};display:inline-block;"></span>
              <span style="font-size:.82rem;color:#111;font-weight:700;">{_pretty_product_name(row['product'])}</span>
            </span>'''
        for _, row in legend.iterrows()
    )
    st.markdown(legend_html, unsafe_allow_html=True)

    with st.expander("Show detailed sales table"):
        table = grouped_sales.copy()
        table["product"] = table["product"].map(_pretty_product_name)
        table = table.rename(columns={"mkp_name": "Market", "product": "Product", "sales": "Sales"})
        st.dataframe(table.style.format({"Sales": "${:,.2f}"}), use_container_width=True)

def render_summary_dashboard():
    inject_dashboard_theme()
    mode = st.session_state.get("app_mode", "shinny")

    # Keep the working weekly forecast behavior from main.py for the default weekly view.
    # main1 was forcing daily forecasts and then grouping them, which distorted sales
    # and made cashflow start too high in some datasets.
    try:
        raw_sales_history, _ = run_sales_forecast(
            periods=1,
            freq="daily",
            mode=mode,
            past_periods_to_show=1,
        )
    except Exception:
        raw_sales_history = pd.DataFrame(columns=["ds", "y"])

    last_sales_date = None
    if not raw_sales_history.empty and "ds" in raw_sales_history.columns:
        last_sales_date = pd.to_datetime(raw_sales_history["ds"]).max()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        group_by = st.selectbox(
            "Group results by",
            options=["Day", "Week", "Month", "Custom days"],
            index=1,
            key="summary_group_by",
        )
    with col2:
        if group_by == "Custom days":
            custom_days = st.number_input(
                "Days per group",
                min_value=1,
                max_value=365,
                value=7,
                step=1,
                key="summary_custom_days",
            )
        else:
            custom_days = _time_span_days(group_by, 7)
            st.number_input(
                "Days per group",
                min_value=1,
                max_value=365,
                value=custom_days,
                step=1,
                key="summary_custom_days_disabled",
                disabled=True,
            )
    with col3:
        default_start = _default_summary_start_date(
            last_sales_date,
            group_by,
            int(custom_days),
            past_spans=4,
        )
        start_date = st.date_input(
            "Start date",
            value=default_start,
            key=f"summary_start_date_{group_by}_{int(custom_days)}",
        )
    with col4:
        forecasted_period = st.number_input(
            "Forecasted period",
            min_value=1,
            max_value=52,
            value=4,
            step=1,
            help="Number of future grouped periods to show. Example: 4 weeks, 4 months, or 4 custom spans.",
            key="summary_forecasted_period",
        )

    start_ts = pd.Timestamp(start_date).normalize()
    last_ts = pd.Timestamp(date.today()).normalize() if last_sales_date is None or pd.isna(last_sales_date) else pd.to_datetime(last_sales_date).normalize()
    selected_past_days = max((last_ts - start_ts).days + 1, _time_span_days(group_by, int(custom_days)) * 4)

    try:
        if group_by == "Week":
            past_periods_to_show = max(1, int((selected_past_days + 6) // 7))
            sales_history, sales_forecast = run_sales_forecast(
                periods=int(forecasted_period),
                freq="weekly",
                mode=mode,
                past_periods_to_show=past_periods_to_show,
            )
            cashflow_history, cashflow_projection = run_cashflow_projection(
                periods=int(forecasted_period),
                freq="weekly",
                mode=mode,
                past_periods_to_show=past_periods_to_show,
            )
        else:
            daily_forecast_periods = _forecast_days_needed(group_by, int(custom_days), int(forecasted_period))
            sales_history, sales_forecast = run_sales_forecast(
                periods=daily_forecast_periods,
                freq="daily",
                mode=mode,
                past_periods_to_show=int(selected_past_days),
            )
            cashflow_history, cashflow_projection = run_cashflow_projection(
                periods=daily_forecast_periods,
                freq="daily",
                mode=mode,
                past_periods_to_show=int(selected_past_days),
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

    if group_by == "Week":
        if not sales_display.empty:
            sales_display["date"] = pd.to_datetime(sales_display["date"])
            sales_display = sales_display[sales_display["date"] >= start_ts].reset_index(drop=True)
        if not cashflow_display.empty:
            cashflow_display["date"] = pd.to_datetime(cashflow_display["date"])
            cashflow_display = cashflow_display[cashflow_display["date"] >= start_ts].reset_index(drop=True)
    else:
        sales_display = _aggregate_forecast_display(
            sales_display,
            group_by=group_by,
            custom_days=int(custom_days),
            start_date=start_date,
            forecasted_periods=int(forecasted_period),
            aggregation="sum",
        )
        cashflow_display = _aggregate_forecast_display(
            cashflow_display,
            group_by=group_by,
            custom_days=int(custom_days),
            start_date=start_date,
            forecasted_periods=int(forecasted_period),
            aggregation="last",
        )

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
    st.caption(
        f"Showing data from **{pd.Timestamp(start_date).date()}**. "
        f"Future view shows the next **{int(forecasted_period)}** {group_by.lower()} period(s)."
    )
    if sales_display.empty:
        st.warning("No sales forecast available.")
    else:
        render_forecast_band_chart(sales_display, title="Sales Summary Forecast")
        with st.expander("Show sales table"):
            render_pretty_table(sales_display)

    st.markdown("### Cash Flow: Real Balances vs Projection")
    if group_by == "Week":
        st.caption("Using the same native weekly cashflow logic as the working main.py file.")
    else:
        st.caption("Cash flow is calculated daily, then grouped by taking the latest balance inside each selected period.")
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
    inject_dashboard_theme()
    render_hero("Forecast Dashboard", "Choose the environment you want to access.", "ShinnySkin Analytics")
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
    inject_dashboard_theme()
    render_hero("ShinnySkin Access", "Enter password to access the private environment.", "Private Dashboard")
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


def render_left_panel():
    inject_dashboard_theme()
    mode = st.session_state.get("app_mode")
    mode_label = "Shinny Skin" if mode == "shinny" else "Test"

    with st.sidebar:
        st.markdown(
            f"""
            <div class="ss-sidebar-rail">
                <div class="ss-sidebar-logo-mark">SS</div>
                <div class="ss-sidebar-copy">
                    <div class="ss-sidebar-brand">SHINNYSKIN</div>
                    <div class="ss-sidebar-mode">{mode_label} dashboard</div>
                </div>
            </div>
            <div class="ss-sidebar-hint">Hover to expand navigation</div>
            """,
            unsafe_allow_html=True,
        )

        if st.button("← Back to selector", key="sidebar_back_to_selector", use_container_width=True):
            _reset_navigation()
            st.rerun()

        st.markdown("---")

        nav_options = [
            ("✦ Summary Dashboard", "summary_dashboard", True),
            ("▦ Detailed Sales", "detailed_sales", True),
            ("↥ Update Data", "update_data", mode == "shinny"),
        ]
        valid_options = {option for _, option, enabled in nav_options if enabled}
        if st.session_state.get("selected_option") not in valid_options:
            st.session_state.selected_option = "summary_dashboard"

        for label, option, enabled in nav_options:
            if not enabled:
                continue
            selected = st.session_state.get("selected_option") == option
            button_label = f"● {label}" if selected else label
            if st.button(button_label, key=f"sidebar_nav_{option}", use_container_width=True):
                st.session_state.selected_option = option
                st.rerun()


def render_home():
    render_left_panel()

def route_page():
    routes = {
        "summary_dashboard": render_summary_dashboard,
        "detailed_sales": render_detailed_sales,
        "update_data": render_update_data,
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
