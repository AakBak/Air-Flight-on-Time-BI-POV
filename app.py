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
    Page("app.py", "Data Preparation", "🛠️"),
    Page("pages/Pyspark-flights.py", "Feature Engineering", "💡"),
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
    df1_uncleaned = pd.read_csv('1991.csv.gz', encoding='ISO-8859-1', compression='gzip')
    df2_uncleaned = pd.read_csv('2001.csv.gz', encoding='ISO-8859-1', compression='gzip')
    return df1_uncleaned, df2_uncleaned

df1_uncleaned, df2_uncleaned= load_data()

# Cleaned data
df1 = df1_uncleaned.drop(columns = ['TailNum', 'AirTime', 'TaxiIn', 'TaxiOut', 'CancellationCode',
        'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay'])
df1 = df1.dropna(axis= 0, how = 'any')
df2 = df2_uncleaned.drop(columns = ['CancellationCode', 'CarrierDelay', 'WeatherDelay', 'NASDelay',
                        'SecurityDelay', 'LateAircraftDelay'])
df2 = df2.dropna(axis= 0, how = 'any')

# # Cleaned data
# df1 = df1_uncleaned.drop(columns = ['Year', 'CRSDepTime','ArrTime','CRSArrTime', 'TailNum', 'CRSElapsedTime', 'AirTime', 'TaxiIn', 'TaxiOut', 'CancellationCode',
#         'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay'])
# df1 = df1.dropna(axis= 0, how = 'any')
# df2 = df2_uncleaned.drop(columns = ['Year', 'CRSDepTime','ArrTime','CRSArrTime', 'CRSElapsedTime', 'CancellationCode', 'CarrierDelay', 'WeatherDelay', 'NASDelay',
#                         'SecurityDelay', 'LateAircraftDelay'])
# df2 = df2.dropna(axis= 0, how = 'any')

# # Save cleaned data to new compressed CSV files
# df1.to_csv('./1991_cleaned_.csv', index=False)
# df2.to_csv('./2001_cleaned.csv', index=False)

# Sidebar
# display_null_values = st.sidebar.checkbox(label="Display Null Values")
# display_dataset = st.sidebar.checkbox(label="Dispaly Dataset")
with st.sidebar.expander("Filter"):
    feature_selection1 = st.multiselect(label="Airline", options = np.union1d(df1['UniqueCarrier'].unique(), df2['UniqueCarrier'].unique()))
    feature_selection2 = st.multiselect(label="Origin", options = np.union1d(df1['Origin'].unique(), df2['Origin'].unique()))
    feature_selection3 = st.multiselect(label="Destination", options = np.union1d(df1['Dest'].unique(), df2['Dest'].unique()))
    A1, A2, A3 = st.columns([1.01, 1, 1])
    with A1:
        feature_selection4 = st.multiselect(label="Year", options = np.union1d(df1['Year'].unique(), df2['Year'].unique()), placeholder='YYYY')
    with A2:
        feature_selection5 = st.multiselect(label="Month", options = np.union1d(df1['Month'].unique(), df2['Month'].unique()), placeholder='MM')
    with A3:
        feature_selection6 = st.multiselect(label="Day", options = np.union1d(df1['DayofMonth'].unique(), df2['DayofMonth'].unique()), placeholder='DD')

# Filter features
query_filter = []
query = ''
if feature_selection1 :
    query_filter.append(f'UniqueCarrier in {feature_selection1}')
if feature_selection2 :
    query_filter.append(f'Origin in {feature_selection2}')
if feature_selection3 :
    query_filter.append(f'Dest in {feature_selection3}')
if feature_selection4:
    query_filter.append(f'Year in {feature_selection4}')
if feature_selection5:
    query_filter.append(f'Month in {feature_selection5}')
if feature_selection6:
    query_filter.append(f'DayofMonth in {feature_selection6}')
query = ' & '.join(query_filter)
df1991 = df1.query(query) if query else df1
df2001 = df2.query(query) if query else df2

# Tabs
tab1, tab2, tab3 = st.tabs(["Data Collection", "Data Cleaning", "Data Exploration"])

# Dashboard design
with st.spinner("Loading..."):
    with tab1:
        A1, A2, A3 = st.columns([1, 0.2, 1])
        with A1:
            st.markdown('<h1 style="text-align:center;color:lightblue;">1991 Flights</h1>', unsafe_allow_html=True)
            st.dataframe(data=df1_uncleaned.head(50), use_container_width=True)
        with A3:
            st.markdown('<h1 style="text-align:center;color:lightblue;">2001 Flights</h1>', unsafe_allow_html=True)
            st.dataframe(data=df2_uncleaned.head(50), use_container_width=True)
    
    with tab2:
        A1, A2, A3 = st.columns([1, 0.2, 1])
        with A1:
            st.markdown('<h1 style="text-align:center;color:lightblue;">1991 Flights</h1>', unsafe_allow_html=True)
            st.plotly_chart(f.plot_null_value_counts(df1_uncleaned), use_container_width=True)
        with A3:
            st.markdown('<h1 style="text-align:center;color:lightblue;">2001 Flights</h1>', unsafe_allow_html=True)
            st.plotly_chart(f.plot_null_value_counts(df2_uncleaned), use_container_width=True)

    with tab3:
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

        # Stacked-bar plot
        A1, A2, A3 = st.columns([1, 0.2, 1])
        with A1:
            st.markdown('<h1 style="text-align:center;"><span style="color:#FF5733;">19</span><span style="color:#8BC34A;">91</span></h1>', unsafe_allow_html=True)
            st.plotly_chart(f.plot_delayed_flights(df1991), use_container_width=True)
        with A3:
            st.markdown('<h1 style="text-align:center;"><span style="color:#FF5733;">20</span><span style="color:#8BC34A;">01</span></h1>', unsafe_allow_html=True)
            st.plotly_chart(f.plot_delayed_flights(df2001), use_container_width=True)
        st.divider()

        # Stacked-bar plot
        A1, A2, A3 = st.columns([1, 0.2, 1])
        with A1:
            st.markdown('<h1 style="text-align:center;"><span style="color:#FF5733;">19</span><span style="color:#8BC34A;">91</span></h1>', unsafe_allow_html=True)
            st.plotly_chart(f.plot_delayed_flights_by_origin(df1991), use_container_width=True)
        with A3:
            st.markdown('<h1 style="text-align:center;"><span style="color:#FF5733;">20</span><span style="color:#8BC34A;">01</span></h1>', unsafe_allow_html=True)
            st.plotly_chart(f.plot_delayed_flights_by_origin(df2001), use_container_width=True)
        st.divider()

        # Stacked-bar plot
        A1, A2, A3 = st.columns([1, 0.2, 1])
        with A1:
            st.markdown('<h1 style="text-align:center;"><span style="color:#FF5733;">19</span><span style="color:#8BC34A;">91</span></h1>', unsafe_allow_html=True)
            st.plotly_chart(f.plot_delayed_flights_by_destination(df1991), use_container_width=True)
        with A3:
            st.markdown('<h1 style="text-align:center;"><span style="color:#FF5733;">20</span><span style="color:#8BC34A;">01</span></h1>', unsafe_allow_html=True)
            st.plotly_chart(f.plot_delayed_flights_by_destination(df2001), use_container_width=True)
        st.divider()

        # Pie chart
        A1, A2, A3 = st.columns([1, 0.2, 1])
        with A1:
            st.markdown('<h1 style="text-align:center;"><span style="color:#FF5733;">19</span><span style="color:#8BC34A;">91</span></h1>', unsafe_allow_html=True)
            st.plotly_chart(f.plot_delay_pie_chart(df1991), use_container_width=True, align='center')
        with A3:
            st.markdown('<h1 style="text-align:center;"><span style="color:#FF5733;">20</span><span style="color:#8BC34A;">01</span></h1>', unsafe_allow_html=True)
            st.plotly_chart(f.plot_delay_pie_chart(df2001), use_container_width=True, align='center')
        st.divider()

        # Line chart
        A1, A2, A3 = st.columns([1, 0.2, 1])
        with A1:
            st.markdown('<h1 style="text-align:center;"><span style="color:darkblue;">19</span><span style="color:yellow;">91</span></h1>', unsafe_allow_html=True)
            st.plotly_chart(f.plot_flights_by_carrier(df1991), use_container_width=True)
        with A3:
            st.markdown('<h1 style="text-align:center;"><span style="color:darkblue;">20</span><span style="color:yellow;">01</span></h1>', unsafe_allow_html=True)
            st.plotly_chart(f.plot_flights_by_carrier(df2001), use_container_width=True)
        st.divider()

# st.dataframe(f.add_columns(df1991).head(10))
# f.add_columns(df2001)