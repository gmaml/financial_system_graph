import os
import pandas as pd
import plotly.express as px
from fredapi import Fred
from bokeh.plotting import figure, show, output_file
from bokeh.io import output_notebook
from bokeh.models import ColumnDataSource
from plotly.io import write_html


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


if __name__ == "__main__":
    series_id = "NCBCEBQ027S"
    # graph_bokeh(series_id)
    graph_ploty()
