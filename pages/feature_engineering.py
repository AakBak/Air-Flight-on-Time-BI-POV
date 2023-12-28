import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objs as go
import functions as f
from numerize.numerize import numerize
from st_pages import Page, show_pages, Section

st.set_page_config(
    page_title="Business Intelligence",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="auto",
    menu_items=None
)

show_pages([
    Page("pages/problem-statement.py", "Problem Statement", "🔍"),
    Page("app.py", "Data Pre-Processing", "⌛"),
    Page("pages/feature_engineering.py", "Feature Engineering", "🛠️"),  
    Page("pages/model.py", "Modelling", "💡")
    ])

video_source = "https://assets.mixkit.co/videos/preview/mixkit-airplane-flying-in-a-red-cloudy-sky-1146-large.mp4"

# Use the HTML video tag inside a st.markdown element
st.markdown(f"""
<div style="position: relative;">
<video autoplay loop muted style="width: 100%; height: 150px; object-fit: cover;">
<source src="{video_source}" type="video/mp4">
</video>
<div style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; font-size: 48px; font-family: Arial; text-shadow: 2px 2px 4px black; background: linear-gradient(to right, white, #f2f2f2, #e6e6e6, #d9d9d9, #cccccc, #bfbfbf, #b3b3b3); -webkit-background-clip: text; color: transparent;">
    1991 | 2001 Flights ✈️
</div>
<div style="position: absolute; bottom: 0; right: 0; margin: 10px; font-size: 14px; font-family: Arial; color: white;">
    <i>by</i> <b>Abed Bakkour</b>
</div>
</div>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():

    df1991_FE = pd.read_csv('engineering/engineering_1991.csv')
    df2001_FE = pd.read_csv('engineering/engineering_2001.csv')

    return df1991_FE, df2001_FE

df1991_FE, df2001_FE = load_data()

with st.spinner("Loading..."):
    A1, A2, A3 = st.columns([1, 0.1, 1])
    with A1:
        st.markdown('<h1 style="text-align:center;color:lightblue;">1991 Flights</h1>', unsafe_allow_html=True)
        st.dataframe(data=df1991_FE, use_container_width=True, hide_index=True)
    with A3:
        st.markdown('<h1 style="text-align:center;color:lightblue;">2001 Flights</h1>', unsafe_allow_html=True)
        st.dataframe(data=df2001_FE, use_container_width=True, hide_index=True)