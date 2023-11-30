import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objs as go
import functions as f
from numerize.numerize import numerize

st.set_page_config(
    page_title="Business Intelligence",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="auto",
    menu_items=None
)

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
    df1 = pd.read_csv('1991.csv.gz', encoding='ISO-8859-1', compression='gzip')
    df2 = pd.read_csv('2001.csv.gz', encoding='ISO-8859-1', compression='gzip')
    # df = pd.concat([df1, df2])
    return df1, df2

df1, df2 = load_data()

# Tabs
tab1, tab2, tab3 = st.tabs(["Data Exploration", "Data Engineering", "Data Modelling"])

# Sidebar
display_null_values = st.sidebar.checkbox(label="Display Null Values")
display_dataset = st.sidebar.checkbox(label="Dispaly Dataset")
with st.sidebar.expander("Filter"):
    feature_selection1 = st.multiselect(label="Airline",
                                            options = np.union1d(df1['UniqueCarrier'].unique(), df2['UniqueCarrier'].unique()))
    feature_selection2 = st.multiselect(label="Origin",
                                            options = np.union1d(df1['Origin'].unique(), df2['Origin'].unique()))
    feature_selection3 = st.multiselect(label="Destination",
                                            options = np.union1d(df1['Dest'].unique(), df2['Dest'].unique()))
# Filter features
query_filter = []
query = ''
if feature_selection1 :
    query_filter.append(f'UniqueCarrier in {feature_selection1}')
if feature_selection2 :
    query_filter.append(f'Origin in {feature_selection2}')
if feature_selection3 :
    query_filter.append(f'Dest in {feature_selection3}')

query = ' & '.join(query_filter)
df1991 = df1.query(query) if query else df1
df2001 = df2.query(query) if query else df2

# Dashboard design
with st.spinner("Loading..."):
    with tab1:
        A1, A2, A3 = st.columns([1, 0.2, 1])
        with A1:
            st.markdown('<h1 style="text-align:center;color:lightblue;">1991</h1>', unsafe_allow_html=True)
            B1, B2 , B3, B4 = st.columns([1, 1, 1, 1])
            with B1:
                st.image('images/airplane.png', use_column_width= 'auto')
                st.metric(label = 'Flights', value = numerize(len(df1991)))
            with B2:
                st.image('images/departures.png', use_column_width= 'auto')
                st.metric(label = 'Origins', value = numerize(len(df1991['Origin'].unique())))
            with B3:
                st.image('images/arrivals.png', use_column_width= 'auto')
                st.metric(label = 'Destinations', value = numerize(len(df1991['Dest'].unique())))
            with B4:
                st.image('images/airlines.png', use_column_width= 'auto')
                st.metric(label = 'Airlines', value = numerize(len(df1991['UniqueCarrier'].unique())))
        with A3:
            st.markdown('<h1 style="text-align:center;color:lightblue;">2001</h1>', unsafe_allow_html=True)
            B1, B2 , B3, B4 = st.columns([1, 1, 1, 1])
            with B1:
                st.image('images/airplane.png', use_column_width= 'auto')
                st.metric(label = 'Flights', value = numerize(len(df2001)))
            with B2:
                st.image('images/departures.png', use_column_width= 'auto')
                st.metric(label = 'Origins', value = numerize(len(df2001['Origin'].unique())))
            with B3:
                st.image('images/arrivals.png', use_column_width= 'auto')
                st.metric(label = 'Destinations', value = numerize(len(df2001['Dest'].unique())))
            with B4:
                st.image('images/airlines.png', use_column_width= 'auto')
                st.metric(label = 'Airlines', value = numerize(len(df2001['UniqueCarrier'].unique())))
        st.divider()

        if display_null_values:
            A1, A2, A3 = st.columns([1, 0.2, 1])
            with A1:
                st.plotly_chart(f.plot_null_value_counts(df1991), use_container_width=True)
            with A3:
                st.plotly_chart(f.plot_null_value_counts(df2001), use_container_width=True)
            st.divider()

        if display_dataset:
            A1, A2, A3 = st.columns([1, 0.2, 1])
            with A1:
                st.dataframe(data=df1991.head(50), use_container_width=True)
            with A3:
                st.dataframe(data=df2001.head(50), use_container_width=True)
            st.divider()

        # Stacked-bar plot
        A1, A2, A3 = st.columns([1, 0.2, 1])
        with A1:
            st.plotly_chart(f.plot_delayed_flights(df1991), use_container_width=True)
        with A3:
            st.plotly_chart(f.plot_delayed_flights(df2001), use_container_width=True)
        st.divider()

        # Pie chart
        A1, A2, A3 = st.columns([1, 0.2, 1])
        with A1:
            st.plotly_chart(f.plot_delay_pie_chart(df1991), use_container_width=True, align='center')
        with A3:
            st.plotly_chart(f.plot_delay_pie_chart(df2001), use_container_width=True, align='center')
        st.divider()

# # Example usage with your specified attributes
# attributes_to_plot = ['DepTime', 'CRSDepTime', 'ArrTime', 'CRSArrTime', 'ActualElapsedTime',
#                       'CRSElapsedTime', 'AirTime', 'ArrDelay', 'DepDelay', 'Distance', 'TaxiIn',
#                       'TaxiOut']

# # Assuming df2 is your DataFrame
# fig = f.create_box_plots(df2[attributes_to_plot])

# # Display the plot using Streamlit
# st.pyplot(fig)