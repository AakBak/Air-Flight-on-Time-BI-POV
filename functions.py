import plotly.graph_objs as go
import plotly.express as px
from plotly.subplots import make_subplots
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

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

def encode_and_assemble(df, categorical_cols, target_col):
    # Define stages for the pipeline
    stages = []

    # Apply StringIndexer to categorical columns
    for col in categorical_cols:
        indexer = StringIndexer(inputCol=col, outputCol=f"{col}_index")
        stages.append(indexer)

    # Apply OneHotEncoder to indexed categorical columns
    encoded_cols = [f"{col}_index" for col in categorical_cols]
    encoder = OneHotEncoder(inputCols=encoded_cols, outputCols=[f"{col}_encoded" for col in categorical_cols])
    stages.append(encoder)

    # Assemble features including one-hot encoded columns
    assembler_inputs = encoded_cols + [target_col]
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    stages.append(assembler)

    # Create and run the pipeline
    pipeline = Pipeline(stages=stages)
    model = pipeline.fit(df)
    transformed_df = model.transform(df)

    return transformed_df

def add_columns (df):
    df['DELAYED'] = np.where(df['ArrDelay'] <= 0, 0, 1)
    return df

def plot_3d_correlation_heatmap(dataset):
    # Exclude columns from the correlation matrix
    excluded_columns = ['Year', 'Month', 'DayofMonth', 'DayOfWeek', 'Cancelled', 'Diverted']
    columns_to_include = [col for col in dataset.columns if col not in excluded_columns]

    # Subset the dataset to include only relevant columns
    subset_dataset = dataset[columns_to_include]

    # Calculate correlation matrix
    correlation_matrix = subset_dataset.corr()

    # Get row and column names for the heatmap
    rows, cols = correlation_matrix.index, correlation_matrix.columns

    # Create a 3D surface plot
    trace = go.Surface(z=correlation_matrix.values,
                      x=cols,
                      y=rows,
                      colorscale='Viridis')

    layout = go.Layout(title='3D Correlation Heatmap',
                       scene=dict(
                           xaxis=dict(title=' ', tickmode='array', tickvals=list(range(len(cols))), ticktext=cols),
                           yaxis=dict(title=' ', tickmode='array', tickvals=list(range(len(rows))), ticktext=rows),
                           zaxis=dict(title='Correlation'),
                           camera=dict(eye=dict(x=-2, y=-2, z=1.25))  # Adjusted the camera position
                       ),
                       width=400,  # Adjust the width of the plot
                       height=700
                        ) # Adjust the height of the plot)

    fig = go.Figure(data=[trace], layout=layout)

    return fig

def plot_correlation_heatmap(dataset):
    # Exclude columns from the correlation matrix
    excluded_columns = ['Year', 'Month', 'DayofMonth', 'DayOfWeek', 'Cancelled', 'Diverted']
    columns_to_include = [col for col in dataset.columns if col not in excluded_columns]

    # Subset the dataset to include only relevant columns
    subset_dataset = dataset[columns_to_include]

    # Calculate correlation matrix
    correlation_matrix = subset_dataset.corr()

    # Set up the matplotlib figure
    fig, ax = plt.subplots(figsize=(10, 8), facecolor='none')

    # Create a heatmap using seaborn
    sns.heatmap(correlation_matrix, annot=True, cmap='viridis', fmt=".2f", linewidths=.5, ax=ax)

    # Set x and y label colors to white
    ax.xaxis.label.set_color('white')
    ax.yaxis.label.set_color('white')

    # Set x and y tick colors to white
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')

    # Customize color bar title and tick labels
    cbar = ax.collections[0].colorbar
    cbar.set_label('Correlation', color='white')
    cbar.ax.yaxis.label.set_color('white')
    cbar.ax.tick_params(axis='y', colors='white')

    # Set the title of the plot
    ax.set_title('Correlation Heatmap', color='white')

    return fig

import plotly.graph_objects as go

def plot_feature_importance(df_combined, model_name):
    # Sort the DataFrame by the model_name column in descending order
    df_combined_sorted = df_combined.sort_values(by=model_name, ascending=True)

    # Create a horizontal bar plot
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df_combined_sorted['Feature'],
        x=df_combined_sorted[model_name],
        orientation='h',
        marker_color=px.colors.sequential.Reds + px.colors.sequential.Reds_r
    ))

    # Customize layout
    fig.update_layout(
        # title=f'{model_name} Feature Importance',
        xaxis_title='Percentage %',
        yaxis_title='Feature',
        margin=dict(l=0, r=0, t=30, b=0)  # Adjust margins as needed
    )

    # Show the Plotly figure using Streamlit
    return fig
