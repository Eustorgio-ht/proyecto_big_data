import streamlit as st
import json
import time
import pandas as pd

st.set_page_config(page_title="Amazon Live Monitor", layout="wide")

st.title("📡 Amazon UK: Real-Time Review Monitor")
st.markdown("Simulando ingesta de eventos mediante pipeline de streaming.")

# Espacios para las métricas
col1, col2, col3 = st.columns(3)
kpi_stars = col1.empty()
kpi_price = col2.empty()
kpi_reviews = col3.empty()

chart_area = st.empty()

# Lista para guardar los datos que van llegando
if 'data_history' not in st.session_state:
    st.session_state.data_history = []

while True:
    try:
        with open('datos/current_event.json', 'r') as f:
            event = json.load(f)
        
        # Evitar duplicados si el productor es lento
        if not st.session_state.data_history or event != st.session_state.data_history[-1]:
            st.session_state.data_history.append(event)
            
            # Limitar historial para no saturar la RAM
            if len(st.session_state.data_history) > 50:
                st.session_state.data_history.pop(0)

        # Convertir historial a DataFrame para gráficas
        hist_df = pd.DataFrame(st.session_state.data_history)

        # Actualizar KPIs
        kpi_stars.metric("Rating Promedio (Live)", f"⭐ {hist_df['stars'].mean():.2f}")
        kpi_price.metric("Precio Promedio", f"£ {hist_df['price'].mean():.2f}")
        kpi_reviews.metric("Total Eventos", len(st.session_state.data_history))

        # Gráfico de línea en tiempo real
        chart_area.line_chart(hist_df['stars'])

    except Exception:
        pass # Esperar a que el archivo se cree
    
    time.sleep(0.5)