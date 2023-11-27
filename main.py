import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objs as go
import functions as f
from PIL import Image
from numerize.numerize import numerize

st.set_page_config(
    page_title="Business Intelligence",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="auto",
    menu_items=None
)

st.title(':rainbow[1991 | 2001 Flights] ✈️')
st.divider()

@st.cache_data
def load_data():
    df1 = pd.read_csv('1991.csv.gz', encoding='ISO-8859-1', compression='gzip')
    df2 = pd.read_csv('2001.csv.gz', encoding='ISO-8859-1', compression='gzip')
    # df = pd.concat([df1, df2])
    return df1, df2

df1, df2 = load_data()

aa1, aa2, aa3 = st.columns([1, 0.2, 1])

with aa1:
    st.markdown('<h1 style="text-align:center;color:lightblue;">1991</h1>', unsafe_allow_html=True)
    a1, a2 , a3, a4 = st.columns([1, 1, 1, 1])

    with a1:
        st.image('images/airplane.png', use_column_width= 'auto')
        st.metric(label = 'Flights', value = numerize(len(df1)))

    with a2:
        st.image('images/departures.png', use_column_width= 'auto')
        st.metric(label = 'Origins', value = numerize(len(df1['Origin'].unique())))

    with a3:
        st.image('images/arrivals.png', use_column_width= 'auto')
        st.metric(label = 'Destinations', value = numerize(len(df1['Dest'].unique())))

    with a4:
        st.image('images/airlines.png', use_column_width= 'auto')
        st.metric(label = 'Airlines', value = numerize(len(df1['UniqueCarrier'].unique())))

with aa3:
    st.markdown('<h1 style="text-align:center;color:lightblue;">2001</h1>', unsafe_allow_html=True)
    a1, a2 , a3, a4 = st.columns([1, 1, 1, 1])

    with a1:
        st.image('images/airplane.png', use_column_width= 'auto')
        st.metric(label = 'Flights', value = numerize(len(df2)))

    with a2:
        st.image('images/departures.png', use_column_width= 'auto')
        st.metric(label = 'Origins', value = numerize(len(df2['Origin'].unique())))

    with a3:
        st.image('images/arrivals.png', use_column_width= 'auto')
        st.metric(label = 'Destinations', value = numerize(len(df2['Dest'].unique())))

    with a4:
        st.image('images/airlines.png', use_column_width= 'auto')
        st.metric(label = 'Airlines', value = numerize(len(df2['UniqueCarrier'].unique())))

st.divider()

check_box = st.sidebar.checkbox(label="Display Null Values")
if check_box:
    c1, c2, c3 = st.columns([1, 0.2, 1])
    with c1:
        st.plotly_chart(f.plot_null_value_counts(df1), use_container_width=True)
    with c3:
        st.plotly_chart(f.plot_null_value_counts(df2), use_container_width=True)
    st.divider()

df1 = df1.drop(columns = ['TailNum', 'AirTime', 'TaxiIn', 'TaxiOut', 'CancellationCode',
           'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay'])
df1 = df1.dropna(axis= 0, how = 'any')

df2 = df2.drop(columns = ['CancellationCode', 'CarrierDelay', 'WeatherDelay', 'NASDelay',
                          'SecurityDelay', 'LateAircraftDelay'])
df2 = df2.dropna(axis= 0, how = 'any')

check_box = st.sidebar.checkbox(label="Dispaly Dataset")
if check_box:
    b1, b2, b3 = st.columns([1, 0.2, 1])
    with b1:
        st.dataframe(data=df1.head(50), use_container_width=True)
    with b3:
        st.dataframe(data=df2.head(50), use_container_width=True)
    st.divider()

st.sidebar.title("Filter")

feature_selection1 = st.sidebar.multiselect(label="Airline",
                                           options = np.union1d(df1['UniqueCarrier'].unique(), df2['UniqueCarrier'].unique()))

feature_selection2 = st.sidebar.multiselect(label="Origin",
                                           options = np.union1d(df1['Origin'].unique(), df2['Origin'].unique()))

feature_selection3 = st.sidebar.multiselect(label="Destination",
                                           options = np.union1d(df1['Dest'].unique(), df2['Dest'].unique()))

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

c1, c2, c3 = st.columns([1, 0.2, 1])
with c1:
    st.plotly_chart(f.plot_delayed_flights(df1991), use_container_width=True)
with c3:
    st.plotly_chart(f.plot_delayed_flights(df2001), use_container_width=True)

st.divider()

c1, c2, c3 = st.columns([1, 0.2, 1])
with c1:
    st.plotly_chart(f.plot_delay_pie_chart(df1), use_container_width=True, align='center')
with c3:
    st.plotly_chart(f.plot_delay_pie_chart(df2), use_container_width=True, align='center')

st.divider()