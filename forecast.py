# Import necessary libraries
import streamlit as st
import pandas as pd
from prophet import Prophet
import plotly.graph_objects as go

# Set up the Streamlit data source
def upload():
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

    st.title("Sales Forecast Dashboard")
    st.caption("Load your sales data and get forecasts for the next 15 periods.")

    with st.container():
        st.markdown("### Load Your Sales Data")
        st.write(
            "The CSV file should have a date column in the format DD/MM/YYYY, "
            "and numerical columns for each item you want to forecast."
        )

        uploaded_file = st.file_uploader(
            " ",
            type=["csv"],
            help="Example of valid date format: 18/01/2020"
        )

    if uploaded_file is None:
        st.info("Load a CSV file to begin forecasting.")
        return None, None

    try:
        ventas = pd.read_csv(uploaded_file)
        date_col = ventas.columns[0]
        ventas[date_col] = pd.to_datetime(
            ventas[date_col],
            format="%d/%m/%Y",
            errors="coerce"
        )

        ventas = ventas.dropna(subset=[date_col]).sort_values(date_col).reset_index(drop=True)

        if ventas.empty:
            st.error("File doesn't contains valid dates in colum {date_col}.")
            return None, None

        if len(ventas) < 5:
            st.error("File must contain at least 5 rows of data.")
            return None, None

        last_date = ventas[date_col].iloc[-5]

        st.success("File loaded properly.")

        col1, col2, col3 = st.columns(3)
        col1.metric("Filas", len(ventas))
        col2.metric("Columnas", len(ventas.columns))
        col3.metric("Última fecha válida", ventas[date_col].max().strftime("%d/%m/%Y"))

        with st.expander("Data Preview", expanded=True):
            st.dataframe(ventas.head(10), use_container_width=True)

        return ventas, last_date

    except KeyError:
        st.error("Date column not found.")
        return None, None

    except Exception as e:
        st.error(f"An error happend while loading the file: {e}")
        return None, None
    

def item_prophet(df, column):
    # Prophet requires columns 'ds' (date) and 'y' (target)
    date_col = df.columns[0]
    df_p = df[[date_col, column]].rename(columns={date_col: 'ds', column: 'y'})
    
    # Initialize and fit the Prophet model
    m = Prophet(yearly_seasonality=True, weekly_seasonality=True)
    m.fit(df_p)

    future = m.make_future_dataframe(periods=15, freq='W')
    forecast = m.predict(future)

    # Unimos el df original (df) con el resultado de prophet (forecast)
    df_final = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].merge(
    df_p.rename(columns={'y': 'real'}), 
    on='ds', 
    how='left'
)
    df_final[['yhat', 'yhat_lower', 'yhat_upper']] =  df_final[['yhat', 'yhat_lower', 'yhat_upper']].clip(lower=0)
    
    return df_final


def pro_plot(df_final, last_date, name):
    df_final = df_final[df_final['ds'] > last_date].copy()

    fig = go.Figure()

    if 'real' in df_final.columns:
        real_data = df_final[df_final['real'].notna()]
        fig.add_trace(go.Scatter(
            x=real_data['ds'],
            y=real_data['real'],
            mode='markers',
            name='Real',
            marker=dict(color='green', size=6)
        ))

    fig.add_trace(go.Scatter(
        x=df_final['ds'],
        y=df_final['yhat_lower'],
        mode='lines',
        line=dict(width=0),
        showlegend=False,
        hoverinfo='skip'
    ))

    fig.add_trace(go.Scatter(
        x=df_final['ds'],
        y=df_final['yhat_upper'],
        mode='lines',
        fill='tonexty',
        fillcolor='rgba(0, 114, 178, 0.2)',
        line=dict(width=0),
        name='Intervalo de Confianza',
        hoverinfo='skip'
    ))

    fig.add_trace(go.Scatter(
        x=df_final['ds'],
        y=df_final['yhat'],
        mode='lines',
        name='Predicción',
        line=dict(color='#0072B2', width=2)
    ))

    fig.update_layout(
        title=f'Pronóstico de Ventas: {name}',
        xaxis_title='Fecha',
        yaxis_title='Unidades / Ventas',
        template='plotly_white',
        yaxis=dict(rangemode='tozero')
    )

    st.plotly_chart(fig, use_container_width=True)

    df = df_final[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
    df.columns = ['Date', 'Forecast', 'Minimum', 'Maximum']
    df.set_index('Date', inplace=True)
    df = df[4:]
    st.write('Forecasted values for the next 15 periods')
    st.dataframe(round(df))

def item_forecast():
    df, last_date = upload()
    try:
        for column in df.columns[1:]:
            item_df = item_prophet(df, column)
            pro_plot(item_df, last_date, column)
    except:
        pass