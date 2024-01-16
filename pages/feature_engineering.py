import streamlit as st
import pandas as pd
import functions as f
from st_pages import Page, show_pages

st.set_page_config(
    page_title="Business Intelligence",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="auto",
    menu_items=None
)

show_pages([
    Page("pages/problem-statement.py", "On-Time Flight Study", "🔍"),
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

    df1_uncleaned = pd.read_csv('1991.csv.gz', encoding='ISO-8859-1', compression='gzip')
    df2_uncleaned = pd.read_csv('2001.csv.gz', encoding='ISO-8859-1', compression='gzip')

    return df1991_FE, df2001_FE, df1_uncleaned, df2_uncleaned

df1991_FE, df2001_FE, df1_uncleaned, df2_uncleaned = load_data()

df1 = df1_uncleaned.drop(columns = ['TailNum', 'AirTime', 'TaxiIn', 'TaxiOut', 'CancellationCode',
        'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay'])
df1 = df1.dropna(axis= 0, how = 'any')
df2 = df2_uncleaned.drop(columns = ['CancellationCode', 'CarrierDelay', 'WeatherDelay', 'NASDelay',
                        'SecurityDelay', 'LateAircraftDelay'])
df2 = df2.dropna(axis= 0, how = 'any')

# Tabs
tab1, tab2 = st.tabs(["Correlation", "Factorization & Standard Scaling"])

with st.spinner("Loading..."):
    with tab1:
        # Radio button for selecting 2D or 3D
        dimension_option = st.radio("Select Dimension:", ["2D", "3D"], horizontal=True)
        # Render plots based on the selected dimension
        if dimension_option == "2D":
            A1, A2, A3 = st.columns([1, 0.2, 1])
            with A1:
                st.markdown('<h1 style="text-align:center;"><span style="color:darkblue;">19</span><span style="color:yellow;">91</span></h1>', unsafe_allow_html=True)
                st.pyplot(f.plot_correlation_heatmap(df1), use_container_width=True)
            with A3:
                st.markdown('<h1 style="text-align:center;"><span style="color:darkblue;">20</span><span style="color:yellow;">01</span></h1>', unsafe_allow_html=True)
                st.pyplot(f.plot_correlation_heatmap(df2), use_container_width=True)
        else:
            A1, A2, A3 = st.columns([1, 0.2, 1])
            with A1:
                st.markdown('<h1 style="text-align:center;"><span style="color:darkblue;">19</span><span style="color:yellow;">91</span></h1>', unsafe_allow_html=True)
                st.plotly_chart(f.plot_3d_correlation_heatmap(df1), use_container_width=True)
            with A3:
                st.markdown('<h1 style="text-align:center;"><span style="color:darkblue;">20</span><span style="color:yellow;">01</span></h1>', unsafe_allow_html=True)
                st.plotly_chart(f.plot_3d_correlation_heatmap(df2), use_container_width=True)

    with tab2:
        A1, A2, A3 = st.columns([1, 0.1, 1])
        with A1:
            st.markdown('<h1 style="text-align:center;color:lightblue;">1991 Flights</h1>', unsafe_allow_html=True)
            st.dataframe(data=df1991_FE, use_container_width=True, hide_index=True)
        with A3:
            st.markdown('<h1 style="text-align:center;color:lightblue;">2001 Flights</h1>', unsafe_allow_html=True)
            st.dataframe(data=df2001_FE, use_container_width=True, hide_index=True)
    