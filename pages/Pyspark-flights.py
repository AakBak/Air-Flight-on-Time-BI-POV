import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objs as go
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as F
from numerize.numerize import numerize
import functions1 as f
from functools import reduce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import isnull, when, count, col

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

# Create a SparkSession with appropriate settings
spark = SparkSession.builder \
    .appName("AirlineDelays") \
    .config('spark.master', 'local[*]') \
    .config("spark.default.parallelism", "16") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
# Define the schema
schema = StructType([
    StructField('Year', IntegerType(), True),
    StructField('Month', IntegerType(), True),
    StructField('DayofMonth', IntegerType(), True),
    StructField('DayOfWeek', IntegerType(), True),
    StructField('DepTime', IntegerType(), True),
    StructField('CRSDepTime', IntegerType(), True),
    StructField('ArrTime', IntegerType(), True),
    StructField('CRSArrTime', IntegerType(), True),
    StructField('UniqueCarrier', StringType(), True),
    StructField('FlightNum', IntegerType(), True),
    StructField('TailNum', StringType(), True),
    StructField('ActualElapsedTime', IntegerType(), True),
    StructField('CRSElapsedTime', IntegerType(), True),
    StructField('AirTime', IntegerType(), True),
    StructField('ArrDelay', IntegerType(), True),
    StructField('DepDelay', IntegerType(), True),
    StructField('Origin', StringType(), True),
    StructField('Dest', StringType(), True),
    StructField('Distance', IntegerType(), True),
    StructField('TaxiIn', IntegerType(), True),
    StructField('TaxiOut', IntegerType(), True),
    StructField('Cancelled', IntegerType(), True),
    StructField('CancellationCode', StringType(), True),
    StructField('Diverted', IntegerType(), True),
    StructField('CarrierDelay', IntegerType(), True),
    StructField('WeatherDelay', IntegerType(), True),
    StructField('NASDelay', IntegerType(), True),
    StructField('SecurityDelay', IntegerType(), True),
    StructField('LateAircraftDelay', IntegerType(), True),
])

# Function to read data using PySpark
@st.cache_resource
def load_data():
    # Read the CSV files into PySpark DataFrames
    df1 = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("compression", "gzip") \
        .schema(schema) \
        .load('1991.csv.gz')

    df2 = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("compression", "gzip") \
        .schema(schema) \
        .load('2001.csv.gz')
    
    # Cleaned data
    df1 = df1.drop(*['TailNum', 'AirTime', 'TaxiIn', 'TaxiOut', 'CancellationCode',
                    'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay']).dropna(how='any')

    df2 = df2.drop(*['CancellationCode', 'CarrierDelay', 'WeatherDelay', 'NASDelay',
                    'SecurityDelay', 'LateAircraftDelay']).dropna(how='any')

    return df1, df2

# Load data
df1, df2 = load_data()

df1_agg = df1.withColumn('DELAYED', when(df1['ArrDelay'] <= 0, 0).otherwise(1))
df2_agg = df2.withColumn('DELAYED', when(df2['ArrDelay'] <= 0, 0).otherwise(1))

# # Assuming 'df_agg' is your DataFrame
# # You can replace this with the actual DataFrame name

# not_delayed_percentage, delayed_percentage = f.calculate_delay_percentages(df1_agg)

# print("Not Delayed Flights Percentage: {:.2%}".format(not_delayed_percentage))
# print("Delayed Flights Percentage: {:.2%}".format(delayed_percentage))


# Tabs
tab1, tab2, tab3 = st.tabs(["Data Preparation", "Data Cleaning", "Data Exploration"])

with st.spinner("Loading..."):
    with tab1:
        A1, A2, A3 = st.columns([1, 0.2, 1])
        with A1:
            st.markdown('<h1 style="text-align:center;color:lightblue;">1991 Flights</h1>', unsafe_allow_html=True)
            st.dataframe(data=df1_agg.limit(50), use_container_width=True)
            # st.plotly_chart(f.plot_delay_pie_chart(df1_agg), use_container_width=True, align='center')
        with A3:
            st.markdown('<h1 style="text-align:center;color:lightblue;">2001 Flights</h1>', unsafe_allow_html=True)
            st.dataframe(data=df2_agg.limit(50), use_container_width=True)
            # st.plotly_chart(f.plot_delay_pie_chart(df2_agg), use_container_width=True, align='center')

            

# # Persist the DataFrames
# df1.persist(StorageLevel.MEMORY_AND_DISK)
# df2.persist(StorageLevel.MEMORY_AND_DISK)

# df1 = df1.repartition(16) 
# df2 = df2.repartition(16)

# from pyspark.sql.functions import isnull, when, count, col
# # Check for duplicates
# df1_duplicates = df1.dropDuplicates()
# df2_duplicates = df2.dropDuplicates()

# # Count the number of duplicates
# num_duplicates1 = df1.count() - df1_duplicates.count()
# num_duplicates2 = df2.count() - df2_duplicates.count()


# # Print the number of duplicates
# print("Number of duplicates:", num_duplicates1, num_duplicates2)

# df_agg = df1.withColumn('DELAY', when(df_agg['DEPARTURE_DELAY'] <= 0, 0).otherwise(1))
# df_agg.show(10)

# # Sidebar
# display_null_values = st.sidebar.checkbox(label="Display Null Values")
# display_dataset = st.sidebar.checkbox(label="Dispaly Dataset")

# # Get distinct values separately for each dataset for each feature
# distinct_values_df1_airline = [row.UniqueCarrier for row in df1.select("UniqueCarrier").distinct().collect()]
# distinct_values_df2_airline = [row.UniqueCarrier for row in df2.select("UniqueCarrier").distinct().collect()]

# distinct_values_df1_origin = [row.Origin for row in df1.select("Origin").distinct().collect()]
# distinct_values_df2_origin = [row.Origin for row in df2.select("Origin").distinct().collect()]

# distinct_values_df1_dest = [row.Dest for row in df1.select("Dest").distinct().collect()]
# distinct_values_df2_dest = [row.Dest for row in df2.select("Dest").distinct().collect()]

# distinct_values_df1_year = [int(row.Year) for row in df1.select("Year").distinct().collect()]
# distinct_values_df2_year = [int(row.Year) for row in df2.select("Year").distinct().collect()]

# distinct_values_df1_month = [int(row.Month) for row in df1.select("Month").distinct().collect()]
# distinct_values_df2_month = [int(row.Month) for row in df2.select("Month").distinct().collect()]

# distinct_values_df1_day = [int(row.DayofMonth) for row in df1.select("DayofMonth").distinct().collect()]
# distinct_values_df2_day = [int(row.DayofMonth) for row in df2.select("DayofMonth").distinct().collect()]
# with st.sidebar.expander("Filter"):
#     # Combine and sort values for each feature
#     feature_selection1 = st.multiselect(label="Airline", options=sorted(set(distinct_values_df1_airline + distinct_values_df2_airline)))
#     feature_selection2 = st.multiselect(label="Origin", options=sorted(set(distinct_values_df1_origin + distinct_values_df2_origin)))
#     feature_selection3 = st.multiselect(label="Destination", options=sorted(set(distinct_values_df1_dest + distinct_values_df2_dest)))
#     A1, A2, A3 = st.columns(3)
#     with A1:
#         feature_selection4 = st.multiselect(label="Year", options=sorted(set(distinct_values_df1_year + distinct_values_df2_year)), placeholder='')
#     with A2:
#         feature_selection5 = st.multiselect(label="Month", options=sorted(set(distinct_values_df1_month + distinct_values_df2_month)), placeholder='')
#     with A3:
#         feature_selection6 = st.multiselect(label="Day", options=sorted(set(distinct_values_df1_day + distinct_values_df2_day)), placeholder='')

# # Filter features
# query_filter = []
# if feature_selection1:
#     query_filter.append(F.col('UniqueCarrier').isin(feature_selection1))
# if feature_selection2:
#     query_filter.append(F.col('Origin').isin(feature_selection2))
# if feature_selection3:
#     query_filter.append(F.col('Dest').isin(feature_selection3))
# if feature_selection4:
#     query_filter.append(F.col('Year').isin(feature_selection4))
# if feature_selection5:
#     query_filter.append(F.col('Month').isin(feature_selection5))
# if feature_selection6:
#     query_filter.append(F.col('DayofMonth').isin(feature_selection6))

# # Apply filters to DataFrames
# df1991 = df1.filter(reduce(lambda x, y: x & y, query_filter)) if query_filter else df1
# df2001 = df2.filter(reduce(lambda x, y: x & y, query_filter)) if query_filter else df2


# # Dashboard design
# with st.spinner("Loading..."):
#     with tab1:
#         A1, A2, A3 = st.columns([1, 0.2, 1])
#         with A1:
#             st.markdown('<h1 style="text-align:center;color:lightblue;">1991</h1>', unsafe_allow_html=True)
#             B1, B2 , B3, B4 = st.columns([1, 1, 1, 1])
#             with B1:
#                 st.image('images/airplane.png', use_column_width= 'auto')
#                 st.metric(label = 'Flights', value = numerize(df1991.count()))
#             with B2:
#                 st.image('images/departures.png', use_column_width= 'auto')
#                 st.metric(label = 'Origins', value = numerize(df1991.agg(F.countDistinct('Origin')).collect()[0][0]))
#             with B3:
#                 st.image('images/arrivals.png', use_column_width= 'auto')
#                 st.metric(label = 'Destinations', value = numerize(df1991.agg(F.countDistinct('Dest')).collect()[0][0]))
#             with B4:
#                 st.image('images/airlines.png', use_column_width= 'auto')
#                 st.metric(label = 'Airlines', value = numerize(df1991.agg(F.countDistinct('UniqueCarrier')).collect()[0][0]))
#         with A3:
#             st.markdown('<h1 style="text-align:center;color:lightblue;">2001</h1>', unsafe_allow_html=True)
#             B1, B2 , B3, B4 = st.columns([1, 1, 1, 1])
#             with B1:
#                 st.image('images/airplane.png', use_column_width= 'auto')
#                 st.metric(label = 'Flights', value = numerize(df2001.count()))
#             with B2:
#                 st.image('images/departures.png', use_column_width= 'auto')
#                 st.metric(label = 'Origins', value = numerize(df2001.agg(F.countDistinct('Origin')).collect()[0][0]))
#             with B3:
#                 st.image('images/arrivals.png', use_column_width= 'auto')
#                 st.metric(label = 'Destinations', value = numerize(df2001.agg(F.countDistinct('Dest')).collect()[0][0]))
#             with B4:
#                 st.image('images/airlines.png', use_column_width= 'auto')
#                 st.metric(label = 'Airlines', value = numerize(df2001.agg(F.countDistinct('UniqueCarrier')).collect()[0][0]))
#         st.divider()

#         if display_null_values:
#             A1, A2, A3 = st.columns([1, 0.2, 1])
#             with A1:
#                 st.plotly_chart(f.plot_null_value_counts(df1991), use_container_width=True)
#             with A3:
#                 st.plotly_chart(f.plot_null_value_counts(df2001), use_container_width=True)
#             st.divider()

#         if display_dataset:
#             A1, A2, A3 = st.columns([1, 0.2, 1])
#             with A1:
#                 st.dataframe(data=df1991.limit(50).toPandas(), use_container_width=True)
#             with A3:
#                 st.dataframe(data=df2001.limit(50).toPandas(), use_container_width=True)
#             st.divider()

#         # Stacked-bar plot
#         A1, A2, A3 = st.columns([1, 0.2, 1])
#         with A1:
#             st.plotly_chart(f.plot_delayed_flights(df1991), use_container_width=True)
#         with A3:
#             st.plotly_chart(f.plot_delayed_flights(df2001), use_container_width=True)
#         st.divider()

#         # Pie chart
#         A1, A2, A3 = st.columns([1, 0.2, 1])
#         with A1:
#             st.plotly_chart(f.plot_delay_pie_chart(df1991), use_container_width=True, align='center')
#         with A3:
#             st.plotly_chart(f.plot_delay_pie_chart(df2001), use_container_width=True, align='center')
#         st.divider()

#         # Line chart
#         A1, A2, A3 = st.columns([1, 0.2, 1])
#         with A1:
#             st.plotly_chart(f.plot_flights_by_carrier(df1991), use_container_width=True)
#         with A3:
#             st.plotly_chart(f.plot_flights_by_carrier(df2001), use_container_width=True)
#         st.divider()

