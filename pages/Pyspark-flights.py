import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objs as go
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as F
from numerize.numerize import numerize
import functions as f
from functools import reduce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import isnull, when, count, col
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

# Create a SparkSession with appropriate settings
spark = SparkSession.builder \
    .appName("AirlineDelays") \
    .config('spark.master', 'local[*]') \
    .config("spark.default.parallelism", "16") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define the schema
schema_1991 = StructType([
    StructField('Year', IntegerType(), True),
    StructField('Month', IntegerType(), True),
    StructField('DayofMonth', IntegerType(), True),
    StructField('DayOfWeek', IntegerType(), True),
    StructField('DepTime', DoubleType(), True),
    StructField('CRSDepTime', IntegerType(), True),
    StructField('ArrTime', DoubleType(), True),
    StructField('CRSArrTime', IntegerType(), True),
    StructField('UniqueCarrier', StringType(), True),
    StructField('FlightNum', IntegerType(), True),
    StructField('ActualElapsedTime', DoubleType(), True),
    StructField('CRSElapsedTime', IntegerType(), True),
    StructField('ArrDelay', DoubleType(), True),
    StructField('DepDelay', DoubleType(), True),
    StructField('Origin', StringType(), True),
    StructField('Dest', StringType(), True),
    StructField('Distance', DoubleType(), True),
    StructField('Cancelled', IntegerType(), True),
    StructField('Diverted', IntegerType(), True),
])

schema_2001 = StructType([
    StructField('Year', IntegerType(), True),
    StructField('Month', IntegerType(), True),
    StructField('DayofMonth', IntegerType(), True),
    StructField('DayOfWeek', IntegerType(), True),
    StructField('DepTime', DoubleType(), True),
    StructField('CRSDepTime', IntegerType(), True),
    StructField('ArrTime', DoubleType(), True),
    StructField('CRSArrTime', IntegerType(), True),
    StructField('UniqueCarrier', StringType(), True),
    StructField('FlightNum', IntegerType(), True),
    StructField('TailNum', StringType(), True),
    StructField('ActualElapsedTime', DoubleType(), True),
    StructField('CRSElapsedTime', IntegerType(), True),
    StructField('AirTime', DoubleType(), True),
    StructField('ArrDelay', DoubleType(), True),
    StructField('DepDelay', DoubleType(), True),
    StructField('Origin', StringType(), True),
    StructField('Dest', StringType(), True),
    StructField('Distance', IntegerType(), True),
    StructField('TaxiIn', IntegerType(), True),
    StructField('TaxiOut', IntegerType(), True),
    StructField('Cancelled', IntegerType(), True),
    StructField('Diverted', IntegerType(), True),
])

# Function to read data using PySpark
@st.cache_resource
def load_data():
    # Read the CSV files into PySpark DataFrames
    df1 = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("compression", "gzip") \
        .schema(schema_1991) \
        .load('1991_cleaned.csv.gz')

    df2 = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("compression", "gzip") \
        .schema(schema_2001) \
        .load('2001_cleaned.csv.gz')

    return df1, df2

# Load data
df1, df2 = load_data()

df1_agg = df1.withColumn('DELAYED', when(df1['ArrDelay'] <= 0, 0).otherwise(1))
df2_agg = df2.withColumn('DELAYED', when(df2['ArrDelay'] <= 0, 0).otherwise(1))

# Tabs
tab1, tab2, tab3 = st.tabs(["Data Preparation", "Data Cleaning", "Data Exploration"])

with st.spinner("Loading..."):
    with tab1:
        A1, A2, A3 = st.columns([1, 0.2, 1])
        with A1:
            st.markdown('<h1 style="text-align:center;color:lightblue;">1991 Flights</h1>', unsafe_allow_html=True)
            st.dataframe(data=df1_agg.limit(50), use_container_width=True)
        with A3:
            st.markdown('<h1 style="text-align:center;color:lightblue;">2001 Flights</h1>', unsafe_allow_html=True)
            st.dataframe(data=df2_agg.limit(50), use_container_width=True)
