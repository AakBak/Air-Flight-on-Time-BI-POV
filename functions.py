import streamlit as st
import pandas as pd
import plotly.graph_objs as go

def plot_delayed_flights(df):
    # Group by airline and calculate the number of delayed and non-delayed flights
    airline_stats = df.groupby('UniqueCarrier') \
        .agg({'ArrDelay': lambda x: sum(x > 0), 'UniqueCarrier': 'size'}) \
        .rename(columns={'ArrDelay': 'Delayed', 'UniqueCarrier': 'Non_Delayed'})

    # Create a stacked bar chart using plotly.graph_objs
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=airline_stats.index,
        y=airline_stats['Non_Delayed'],
        name='Non-Delayed Flights',
        marker=dict(color='limegreen')
    ))

    fig.add_trace(go.Bar(
        x=airline_stats.index,
        y=airline_stats['Delayed'],
        name='Delayed Flights',
        marker=dict(color='orangered')
    ))

    fig.update_layout(
        title=dict(text='Delayed and Non-Delayed Flights per Unique Carrier', font=dict(color='grey')),
        xaxis=dict(title='Unique Carrier'),
        yaxis=dict(title='Number of Flights'),
        barmode='stack',
        width=750,
        height=500,
        legend=dict(x=0.87, y=1.2, orientation='v')
    )

    return fig

def plot_delay_pie_chart(df):
    # Calculate the number of delayed and non-delayed flights
    delayed_flights = df[df['ArrDelay'] > 0].shape[0]
    non_delayed_flights = df[df['ArrDelay'] <= 0].shape[0]

    # Define the labels, sizes, and colors for the pie chart
    labels = ['Delayed Flights', 'Non-Delayed Flights']
    sizes = [delayed_flights, non_delayed_flights]
    colors = ['#FF5733', '#8BC34A']

    # Create the 3D pie chart
    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=sizes,
        pull=[0.1, 0],
        marker=dict(colors=colors),
        textposition='outside',
        textinfo='label+percent',
        insidetextorientation='radial'
    )])

    fig.update_layout(
        title=dict(text='Percentage of Delayed and Non-Delayed Flights', font=dict(color='grey')),
        width=750,
        height=500,
        showlegend=True,
        legend=dict(x=0.41, y=1.12, orientation='h')
    )

    return fig

def plot_null_value_counts(df):
    # Count the number of null values in each column
    null_counts = [df[c].isnull().sum() for c in df.columns]

    # Create a bar chart using plotly.graph_objs
    fig = go.Figure(
        go.Bar(x=df.columns, y=null_counts, marker_color='dimgray')
    )

    # Add x-axis and y-axis labels
    fig.update_layout(
        title=dict(text='Null Value Counts for Each Attribute',font=dict(color='grey')),
        xaxis_title='Attribute',
        yaxis_title='Number of Null Values',
        width=750,
        height=500
    )

    # Rotate x-axis tick labels by 90 degrees
    fig.update_layout(xaxis_tickangle=-90)

    # Display the Plotly figure using st.plotly_chart
    return fig