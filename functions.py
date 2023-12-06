import plotly.graph_objs as go
import plotly.express as px
from plotly.subplots import make_subplots


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
        title=dict(text='Delayed and Non-Delayed Flights by Airlines', font=dict(color='grey')),
        xaxis=dict(title='Airlines'),
        yaxis=dict(title='Number of Flights'),
        barmode='stack',
        width=750,
        height=500,
        legend=dict(x=0.77, y=1.2, orientation='v')
    )

    return fig

def plot_delayed_flights_by_origin(df):
    # Group by airline and calculate the number of delayed and non-delayed flights
    airline_stats = df.groupby('Origin') \
        .agg({'ArrDelay': lambda x: sum(x > 0), 'Origin': 'size'}) \
        .rename(columns={'ArrDelay': 'Delayed', 'Origin': 'Non_Delayed'})

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
        title=dict(text='Delayed and Non-Delayed Flights by Origins', font=dict(color='grey')),
        xaxis=dict(title='Origins'),
        yaxis=dict(title='Number of Flights'),
        barmode='stack',
        width=750,
        height=500,
        legend=dict(x=0.77, y=1.2, orientation='v')
    )

    return fig

def plot_delayed_flights_by_destination(df):
    # Group by airline and calculate the number of delayed and non-delayed flights
    airline_stats = df.groupby('Dest') \
        .agg({'ArrDelay': lambda x: sum(x > 0), 'Dest': 'size'}) \
        .rename(columns={'ArrDelay': 'Delayed', 'Dest': 'Non_Delayed'})

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
        title=dict(text='Delayed and Non-Delayed Flights by Destinations', font=dict(color='grey')),
        xaxis=dict(title='Destinations'),
        yaxis=dict(title='Number of Flights'),
        barmode='stack',
        width=750,
        height=500,
        legend=dict(x=0.77, y=1.2, orientation='v')
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
        # title=dict(text='Percentage of Delayed and Non-Delayed Flights', font=dict(color='grey'), y=1),
        width=750,
        height=500,
        showlegend=False,
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
        title=dict(text='Null Value Counts for Each Attribute',font=dict(color='grey'), x=0.3, y=0.9),
        xaxis_title='Attributes',
        width=100,
        height=400
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

def plot_flights_by_carrier(df):
    # Check if there's any numerical column for counting flights
    numerical_columns = df.select_dtypes(include=['number']).columns
    if not numerical_columns.any():
        raise ValueError("No numerical columns found for counting flights.")

    # Group flights by month and carrier and count the number of flights
    flights_per_month_carrier = df.groupby(['Month', 'UniqueCarrier'])[numerical_columns[0]].count().reset_index()

    # Group delayed flights by month, carrier, and count the number of delayed flights
    delayed_flights_per_month_carrier = df[df['ArrDelay'] > 0].groupby(['Month', 'UniqueCarrier'])['ArrDelay'].count().reset_index()

    # Convert to Pandas dataframes for visualization
    flights_per_month_carrier_pd = flights_per_month_carrier.copy()
    delayed_flights_per_month_carrier_pd = delayed_flights_per_month_carrier.copy()

    # Create subplots with shared x-axis
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.15)

    # Get unique carriers and assign rainbow colors
    unique_carriers = df['UniqueCarrier'].unique()
    rainbow_colors = px.colors.sequential.Plasma
    colors_assigned = dict(zip(unique_carriers, rainbow_colors[:len(unique_carriers)]))

    # Iterate through unique carriers and add traces for non-delayed flights
    for carrier in unique_carriers:
        carrier_data = flights_per_month_carrier_pd[flights_per_month_carrier_pd['UniqueCarrier'] == carrier]
        trace_flights = go.Scatter(
            x=carrier_data['Month'],
            y=carrier_data[numerical_columns[0]],
            mode='lines',
            name=f'{carrier}',
            line=dict(color=colors_assigned.get(carrier, rainbow_colors[0]))
        )
        fig.add_trace(trace_flights, row=1, col=1)

    # Iterate through unique carriers and add traces for delayed flights
    for carrier in unique_carriers:
        delayed_data = delayed_flights_per_month_carrier_pd[
            (delayed_flights_per_month_carrier_pd['UniqueCarrier'] == carrier)
        ]
        trace_delayed = go.Scatter(
            x=delayed_data['Month'],
            y=delayed_data['ArrDelay'],
            mode='lines',
            name=f'{carrier}',
            line=dict(color=colors_assigned.get(carrier, rainbow_colors[0]), dash='dash')
        )
        fig.add_trace(trace_delayed, row=2, col=1)

    # Add a title and labels to the plot
    fig.update_layout(
        title=dict(text='Monthly Trends of Delayed and non-Delayed by Airlines', font=dict(color='grey')),
        xaxis=dict(title='Month', side= 'bottom'),
        yaxis=dict(title=f'Number of Non-Delayed Flights'),
        yaxis2=dict(title=f'Number of Delayed Flights'),
        width=750,
        height=500,
    )

    return fig

from pyspark.sql import functions as F

def calculate_delay_percentages(df, delay_col='DELAYED'):
    
    total_flights = df.count()
    
    not_delayed_count = df.filter(F.col(delay_col) == 0).count()
    not_delayed_percentage = not_delayed_count / total_flights
    
    delayed_percentage = 1 - not_delayed_percentage
    
    return not_delayed_percentage, delayed_percentage