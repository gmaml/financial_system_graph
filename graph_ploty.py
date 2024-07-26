import os
import pandas as pd
import plotly.express as px
from fredapi import Fred
from bokeh.plotting import figure, show, output_file
from bokeh.io import output_notebook
from bokeh.models import ColumnDataSource


def graph_ploty():
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
        title={"x": 0.5},
        xaxis=dict(
            rangeselector=dict(
                buttons=list(
                    [
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(count=6, label="6m", step="month", stepmode="backward"),
                        dict(count=1, label="YTD", step="year", stepmode="todate"),
                        dict(count=1, label="1y", step="year", stepmode="backward"),
                        dict(step="all"),
                    ]
                )
            ),
            rangeslider=dict(visible=True),
            type="date",
        ),
    )

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


if __name__ == "__main__":
    series_id = "NCBCEBQ027S"
    graph_bokeh(series_id)
    # graph_ploty()
