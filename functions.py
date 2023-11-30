import streamlit as st
import pandas as pd
import numpy as np
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
        title=dict(text='Delayed and Non-Delayed Flights per Airline', font=dict(color='grey')),
        xaxis=dict(title='Airlines'),
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
        textposition= 'inside',
        textinfo='label+percent',
        insidetextorientation='radial',
        textfont_color= ['black', 'black']
    )])

    fig.update_layout(
        title=dict(text='Percentage of Delayed and Non-Delayed Flights', font=dict(color='grey'), y=1),
        width=750,
        height=500,
        showlegend=True,
        legend=dict(y=1.12, orientation='v')
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
        title=dict(text='Null Value Counts for Each Attribute',font=dict(color='grey'), x=0.3, y=0.8),
        xaxis_title='Attributes',
        width=100,
        height=300
    )

    # Rotate x-axis tick labels by 90 degrees
    fig.update_layout(xaxis_tickangle=-45)

    # Display the Plotly figure using st.plotly_chart
    return fig

# import matplotlib.pyplot as plt
# import seaborn as sb

# def create_box_plots(df):
#     # Get the number of columns in the DataFrame
#     num_columns = df.shape[1]

#     # Calculate the number of rows and columns for subplots
#     num_rows = 1
#     num_cols = min(num_columns, 3)  # Display up to 3 columns in each row

#     # Calculate the number of rows needed
#     num_rows = (num_columns + num_cols - 1) // num_cols

#     # Create figure and subplots with a transparent background
#     fig, axs = plt.subplots(num_rows, num_cols, figsize=(num_cols * 5, num_rows * 4), tight_layout=True, facecolor='none')

#     # If there is only one column, axs is a single Axes object, so convert it to a 2D list
#     axs = [axs] if num_rows == 1 else axs

#     # Iterate over columns and create a boxplot on each subplot
#     for i in range(num_columns):
#         row_index = i // num_cols
#         col_index = i % num_cols
#         sb.boxplot(y=df.iloc[:, i], showmeans=True, orient='v', color='#8B4513',  # Brown color
#                    boxprops=dict(color='brown'),  # Color of the box
#                    medianprops=dict(color='white'),  # Color of the median line
#                    whiskerprops=dict(color='white'),  # Color of the whisker lines
#                    capprops=dict(color='white'),  # Color of the caps on the whisker lines
#                    flierprops=dict(markeredgecolor='white', markerfacecolor='white'),  # Color of the outliers
#                    meanprops={"marker": "o", "markerfacecolor": "white", "markeredgecolor": "white"},  # Color of the mean point
#                    saturation=0.5, ax=axs[row_index, col_index])
#         # Add grid to each subplot
#         axs[row_index, col_index].grid(True, color="white", linewidth="0.5", linestyle="-.", alpha=0.3)
#         axs[row_index, col_index].set_ylabel(df.columns[i], color='white')  # Set ylabel as title along y-axis

#         # Set background color to be transparent
#         axs[row_index, col_index].set_facecolor('none')

#         # Set y-axis label color
#         axs[row_index, col_index].yaxis.label.set_color('white')

#     # Set common xlabel along x-axis
#     axs[0, 0].set_xlabel('Value', color='white')

#     return fig
