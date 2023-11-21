import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(page_title="Business Intelligence", page_icon=None, layout="wide", initial_sidebar_state="auto", menu_items=None)

st.title('1991 Flights')

@st.cache_data
def load_data():
    return pd.read_csv('1991.csv')

df= load_data()
st.dataframe(data=df.head(5), use_container_width=True)