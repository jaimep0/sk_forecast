# Import necessary libraries
import streamlit as st
import pandas as pd
from prophet import Prophet
import plotly.graph_objects as go

# Set up the Streamlit data source
def upload():
    uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])

    if uploaded_file is not None:
        # Preprocess the data for Prophet
        ventas = pd.read_csv(uploaded_file)
        ventas['Semana'] = pd.to_datetime(ventas['Semana'], format='%d/%m/%Y')
        last_date = ventas['Semana'].iloc[-5]

        st.write("File loaded successfully")
        st.dataframe(ventas.head())

        return ventas, last_date
    else:
        st.write("Please upload a CSV file to proceed.")
        return None, None   
    

def item_prophet(df, column):
    # Prophet requires columns 'ds' (date) and 'y' (target)
    df_p = df[['Semana', column]].rename(columns={'Semana': 'ds', column: 'y'})
    
    # Initialize and fit the Prophet model
    m = Prophet(yearly_seasonality=True, weekly_seasonality=True)
    m.fit(df_p)

    future = m.make_future_dataframe(periods=15, freq='W')
    forecast = m.predict(future)
    
    # Visualize the forecast trend and seasonal patterns
    #if performance:
    #    fig1 = m.plot(forecast)

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
    df.set_index('ds', inplace=True)
    df.columns = ['Pronostico', 'Minimo', 'Máximo']
    st.write('Pronóstico')
    st.dataframe(round(df))

def item_forecast():
    df, last_date = upload()
    try:
        for column in df.columns[1:]:
            item_df = item_prophet(df, column)
            pro_plot(item_df, last_date, column)
    except:
        pass