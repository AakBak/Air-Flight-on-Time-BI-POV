import streamlit as st
import pandas as pd
import plotly.graph_objs as go
import functions as f

st.set_page_config(
    page_title="Business Intelligence",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="auto",
    menu_items=None
)

st.title(':rainbow[1991 & 2001 Flights] ✈️')
st.divider()

@st.cache_data
def load_data():
    return pd.read_csv('1991.csv')

df = load_data()

st.markdown("# 1. Loading Data")
st.dataframe(data=df.head(5), use_container_width=True)

st.markdown("# 2. Data Exploration")
st.plotly_chart(f.plot_delayed_flights(df), use_container_width=True)
st.plotly_chart(f.plot_delay_pie_chart(df), use_container_width=True)
st.plotly_chart(f.plot_null_value_counts(df), use_container_width=True)
