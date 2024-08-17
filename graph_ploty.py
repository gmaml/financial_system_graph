import os
import pandas as pd
import plotly.express as px
from fredapi import Fred
from bokeh.plotting import figure, show, output_file
from bokeh.io import output_notebook
from bokeh.models import ColumnDataSource
from plotly.io import write_html
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def graph_ploty_simple():
    fred = Fred(api_key=os.environ["FRED_API_KEY"])
    data = fred.get_series("NCBCEBQ027S")

    # Convert the Series to a DataFrame
    data_frame = data.reset_index()
    data_frame.columns = ["Date", "Value"]

    # Create a bar plot with a name for the series
    fig = px.bar(
        data_frame,
        x="Date",
        y="Value",
        title="Net equity issues",
        labels={"Value": "Net Equity"},
    )

    # Center the title
    # Center the title
    fig.update_layout(
        title={"text": "<b>Net equity issues</b>", "x": 0.5},
        xaxis=dict(
            rangeselector=dict(
                buttons=list(
                    [
                        dict(count=1, label="1y", step="year", stepmode="backward"),
                        dict(count=2, label="2y", step="year", stepmode="backward"),
                        dict(count=5, label="5Y", step="year", stepmode="todate"),
                        dict(count=10, label="10y", step="year", stepmode="backward"),
                        dict(step="all"),
                    ]
                )
            ),
            rangeslider=dict(
                visible=True,
                bgcolor="lightgray",  # Background color of the range slider
                bordercolor="black",  # Border color of the range slider
                borderwidth=2,  # Border width of the range slider
                thickness=0.1,  # Thickness of the range slider
            ),
            type="date",
        ),
    )

    # Save the plot as an HTML file
    write_html(fig, "net_equity_issues_ploty.html")

    # Show the plot
    fig.show()


def graph_bokeh(series_id):
    # Initialize FRED API
    fred = Fred(api_key=os.environ["FRED_API_KEY"])

    # Download the data from FRED
    data = fred.get_series(series_id)

    # Convert the Series to a DataFrame
    data_frame = data.reset_index()
    data_frame.columns = ["Date", "Value"]

    # Prepare the data for Bokeh
    source = ColumnDataSource(data_frame)

    # Create a bar plot
    p = figure(
        x_axis_type="datetime",
        title="Net equity issues",
        height=400,
        width=700,
        tools="pan,wheel_zoom,box_zoom,reset",
        active_scroll="wheel_zoom",
    )

    p.vbar(x="Date", top="Value", width=0.9, source=source)

    # Center the title
    p.title.align = "center"

    # Show the plot

    # output_notebook()
    # Set the output file
    output_file("net_equity_issues.html")

    show(p)


def adjust_annotations_within_bounds(annotations, x_min, x_max):
    """Adjust annotations to be within the x-axis bounds."""
    adjusted_annotations = []
    for annotation in annotations:
        if annotation["x"] < x_min:
            annotation["x"] = x_min
        elif annotation["x"] > x_max:
            annotation["x"] = x_max
        adjusted_annotations.append(annotation)
    return adjusted_annotations


def complex_graph_ploty(series_dic):
    # Load the Parquet file into a pandas DataFrame
    z1df = pd.read_parquet("z1df.parquet")

    equity_name = "LM893064105"
    total_equity = z1df[z1df["Series_name"].str.contains(equity_name + ".Q", na=False)]

    pivoted_total_equity = total_equity.pivot(
        index="Date", columns="Series_name", values="Obs_value"
    )
    pivoted_total_equity = pivoted_total_equity[["LM893064105.Q"]]
    pivoted_total_equity.reset_index(inplace=True)
    pivoted_total_equity.reset_index(drop=True, inplace=True)

    series_names = [x + ".Q" for x in list(series_dic.keys())]
    equity_holder = z1df[z1df["Series_name"].isin(series_names)]

    pivoted_equity_holder = equity_holder.pivot(
        index="Date", columns="Series_name", values="Obs_value"
    )
    pivoted_equity_holder = pivoted_equity_holder[series_names]
    pivoted_equity_holder.reset_index(inplace=True)

    merged_df = pivoted_equity_holder.merge(pivoted_total_equity, on="Date")
    merged_df.replace(-9999.0, pd.NA, inplace=True)
    merged_df["Date"] = pd.to_datetime(merged_df["Date"])
    filtered_df = merged_df[merged_df["Date"].dt.year > 1952]

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Define grayscale colors
    grayscale_colors = ["#000000", "#333333", "#666666", "#999999", "#CCCCCC"]

    # Add area traces for each series in series_names using grayscale colors
    for i, series in enumerate(series_names):
        fig.add_trace(
            go.Scatter(
                x=filtered_df["Date"],
                y=filtered_df[series],
                mode="lines",
                stackgroup="one",
                name=series,
                line=dict(color=grayscale_colors[i % len(grayscale_colors)]),
            ),
            secondary_y=False,
        )

    # Add a line trace for 'LM893064105.Q' on the secondary y-axis using a grayscale color
    fig.add_trace(
        go.Scatter(
            x=filtered_df["Date"],
            y=filtered_df["LM893064105.Q"],
            mode="lines",
            name="LM893064105.Q",
            line=dict(color="#000000"),  # Black color for the secondary y-axis line
        ),
        secondary_y=True,
    )

    # Add annotations to label the graph area
    annotations = []
    for i, series in enumerate(series_names):
        if i == 0:
            cum_y = filtered_df[series]
        else:
            cum_y += filtered_df[series]

        # Find the index where the difference is the largest
        max_index = filtered_df[series].idxmax()

        # Ensure max_index is within bounds

        if max_index >= len(filtered_df):
            max_index = len(filtered_df) - 1

        annotation_st = -20  #

        max_x_index = max_index + annotation_st

        if max_x_index < 0:
            max_x_index = 0

        annotations.append(
            dict(
                x=filtered_df["Date"].iloc[
                    max_x_index
                ],  # Position the annotation at the point with the most space
                y=cum_y.iloc[max_index] - 0.5 * filtered_df[series].iloc[max_index],
                text=series_dic[
                    series[:-2]
                ],  # Use the original series name without ".Q"
                showarrow=False,
                xanchor="left",
                font=dict(color=grayscale_colors[i % len(grayscale_colors)]),
            )
        )

    # Assuming x-axis data ranges from '2022-01-01' to '2024-12-31'
    x_min = pd.to_datetime("1953-01-01")
    x_max = pd.to_datetime("2024-12-31")

    adjusted_annotations = adjust_annotations_within_bounds(annotations, x_min, x_max)

    # Add adjusted annotations
    for annotation in adjusted_annotations:
        fig.add_annotation(
            x=annotation["x"], y=annotation["y"], text=annotation["text"]
        )

    fig.update_layout(
        title={
            "text": "Area Plot of Series with Secondary Y-Axis for LM893064105.Q",
            "y": 0.9,
            "x": 0.5,
            "xanchor": "center",
            "yanchor": "top",
            "font": {"size": 20, "family": "Arial", "color": "black"},
        },
        xaxis_title="Date",
        yaxis_title="Series Values",
        yaxis2_title="LM893064105.Q",
        xaxis=dict(
            rangeselector=dict(
                buttons=list(
                    [
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(count=6, label="6m", step="month", stepmode="backward"),
                        dict(count=1, label="1y", step="year", stepmode="backward"),
                        dict(count=5, label="5y", step="year", stepmode="backward"),
                        dict(step="all"),
                    ]
                )
            ),
            rangeslider=dict(visible=True),
            type="date",
        ),
        annotations=adjusted_annotations,
        showlegend=False,
    )

    fig.show()


if __name__ == "__main__":
    series_id = "NCBCEBQ027S"
    # graph_bokeh(series_id)
    # graph_ploty_simple()

    # Create the dictionary
    series_dic = {
        "LM153064105": "Household sector",
        "LM103064103": "Nonfinancial corporate business",
        "LM313064105": "Federal government",
        "LM213064103": "State and local governments",
        "FL713064103": "Monetary authority",
        "LM763064103": "U.S.-chartered depository institutions",
        "FL753064103": "Foreign banking offices in U.S.",
        "LM513064105": "Property-casualty insurance companies",
        "LM543064105": "Life insurance companies",
        "LM573064105": "Private pension funds",
        "LM343064105": "Federal government retirement funds",
        "LM223064145": "State and local govt. retirement funds",
        "LM653064100": "Mutual funds",
        "LM553064103": "Closed-end funds",
        "LM563064100": "Exchange-traded funds",
        "LM663064103": "Brokers and dealers",
        "FL503064105": "Other financial business",
        "LM263064105": "Rest of the world",
    }
    complex_graph_ploty(series_dic)
