import streamlit as st
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as F
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

# st.title(':rainbow[1991 | 2001 Flights] ✈️')

# import streamlit as st

# # Replace the video source with the URL of the video
# video_source = "https://assets.mixkit.co/videos/preview/mixkit-military-planes-flying-and-dropping-fireworks-1147-large.mp4"

# # Use the HTML video tag inside a st.markdown element
# st.markdown(f"""
# <video autoplay loop muted style="width: 100%; height: 150px; object-fit: cover;">
# <source src="{video_source}" type="video/mp4">
# </video>
# """, unsafe_allow_html=True)


import streamlit as st

# Replace the video source with your own video path
video_source = "https://assets.mixkit.co/videos/preview/mixkit-airplane-flying-in-a-red-cloudy-sky-1146-large.mp4"

# Use the HTML video tag inside a st.markdown element
st.markdown(f"""
<div style="position: relative;">
<video autoplay loop muted style="width: 100%; height: 150px; object-fit: cover;">
<source src="{video_source}" type="video/mp4">
</video>
<div style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; font-size: 48px; font-family: Arial; text-shadow: 2px 2px 4px black; background: linear-gradient(to right, #ff9900, #ffcc00, #99cc00, #33cc33, #3399cc, #3366cc, #9933cc); -webkit-background-clip: text; color: transparent;">
    1991 | 2001 Flights ✈️
</div>
</div>
""", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["Data Exploration", "Data Engineering", "Data Modelling"])

# Create a SparkSession with appropriate settings
spark = SparkSession.builder \
    .appName("AirlineDelays") \
    .config('spark.master', 'local[*]') \
    .config("spark.default.parallelism", "16") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Function to read data using PySpark
@st.cache_resource
def load_data():
    # Read the CSV files into PySpark DataFrames
    df1 = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("compression", "gzip") \
        .load('1991.csv.gz')

    df2 = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("compression", "gzip") \
        .load('2001.csv.gz')

    return df1, df2

# Load data
df1, df2 = load_data()

# Persist the DataFrames
df1.persist(StorageLevel.MEMORY_AND_DISK)
df2.persist(StorageLevel.MEMORY_AND_DISK)

df1 = df1.repartition(16) 
df2 = df2.repartition(16)

with tab1:

    aa1, aa2, aa3 = st.columns([1, 0.2, 1])

    with aa1:
        st.markdown('<h1 style="text-align:center;color:lightblue;">1991</h1>', unsafe_allow_html=True)
        a1, a2 , a3, a4 = st.columns([1, 1, 1, 1])

        with a1:
            st.image('images/airplane.png', use_column_width= 'auto')
            st.metric(label = 'Flights', value = numerize(df1.count()))

        with a2:
            st.image('images/departures.png', use_column_width= 'auto')
            st.metric(label = 'Origins', value = numerize(df1.agg(F.countDistinct('Origin')).collect()[0][0]))
                
        with a3:
            st.image('images/arrivals.png', use_column_width= 'auto')
            st.metric(label = 'Destinations', value = numerize(df1.agg(F.countDistinct('Dest')).collect()[0][0]))

        with a4:
            st.image('images/airlines.png', use_column_width= 'auto')
            st.metric(label = 'Airlines', value = numerize(df1.agg(F.countDistinct('UniqueCarrier')).collect()[0][0]))

    with aa3:
        st.markdown('<h1 style="text-align:center;color:lightblue;">2001</h1>', unsafe_allow_html=True)
        a1, a2 , a3, a4 = st.columns([1, 1, 1, 1])

        with a1:
            st.image('images/airplane.png', use_column_width= 'auto')
            st.metric(label = 'Flights', value = numerize(df2.count()))

        with a2:
            st.image('images/departures.png', use_column_width= 'auto')
            st.metric(label = 'Origins', value = numerize(df2.agg(F.countDistinct('Origin')).collect()[0][0]))
                
        with a3:
            st.image('images/arrivals.png', use_column_width= 'auto')
            st.metric(label = 'Destinations', value = numerize(df2.agg(F.countDistinct('Dest')).collect()[0][0]))

        with a4:
            st.image('images/airlines.png', use_column_width= 'auto')
            st.metric(label = 'Airlines', value = numerize(df2.agg(F.countDistinct('UniqueCarrier')).collect()[0][0]))

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

    with st.sidebar.expander("Filter"):

        feature_selection1 = st.multiselect(label="Airline",
                                                options = np.union1d(df1['UniqueCarrier'].unique(), df2['UniqueCarrier'].unique()))

        feature_selection2 = st.multiselect(label="Origin",
                                                options = np.union1d(df1['Origin'].unique(), df2['Origin'].unique()))

        feature_selection3 = st.multiselect(label="Destination",
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


# # Example usage with your specified attributes
# attributes_to_plot = ['DepTime', 'CRSDepTime', 'ArrTime', 'CRSArrTime', 'ActualElapsedTime',
#                       'CRSElapsedTime', 'AirTime', 'ArrDelay', 'DepDelay', 'Distance', 'TaxiIn',
#                       'TaxiOut']

# # Assuming df2 is your DataFrame
# fig = f.create_box_plots(df2[attributes_to_plot])

# # Display the plot using Streamlit
# st.pyplot(fig)
