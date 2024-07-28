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


def complex_graph_ploty(series_dic):
    # using ploty to plot figure 2.7
    # Load the Parquet file into a pandas DataFrame
    z1df = pd.read_parquet("z1df.parquet")

    equity_name = "LM893064105"
    total_equity = z1df[
        z1df["Series_name"].str.contains("LM893064105" + ".Q", na=False)
    ]

    series_names = [x + ".Q" for x in list(series_dic.keys())]
    equity_holder = z1df[z1df["Series_name"].isin(series_names)]

    # Assuming 'Date' is a column in your DataFrame
    # Pivot the DataFrame
    pivoted_equity_holder = equity_holder.pivot(
        index="Date", columns="Series_name", values="Obs_value"
    )

    # Select only the 'LM893064105.Q' column
    pivoted_equity_holder = pivoted_equity_holder[series_names]

    # Reset the index to drop the current index and convert it to columns
    pivoted_equity_holder.reset_index(inplace=True)

    # Reset the index again to add a simple integer index
    pivoted_equity_holder.reset_index(drop=True, inplace=True)

    # Rename the index to 'index'
    pivoted_equity_holder.rename_axis("index", inplace=True)

    for series in series_names:
        pivoted_equity_holder[series] = (
            pivoted_equity_holder[series] / total_equity["LM893064105.Q"] * 100
        )

    # Assuming both DataFrames have a 'Date' column
    merged_df = pivoted_equity_holder.merge(total_equity, on="Date")

    # Replace -9999.0 with NaN in the merged DataFrame
    merged_df.replace(-9999.0, np.nan, inplace=True)

    # Melt the DataFrame to long format for Plotly
    melted_df = merged_df.melt(
        id_vars=["Date"], value_vars=series_names, var_name="Series", value_name="Value"
    )

    # Create a subplot with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add area traces for each series in series_names
    for series in series_names:
        fig.add_trace(
            go.Scatter(
                x=merged_df["Date"],
                y=merged_df[series],
                mode="lines",
                stackgroup="one",
                name=series,
            ),
            secondary_y=False,
        )

    # Add a line trace for 'LM893064105.Q' on the secondary y-axis
    fig.add_trace(
        go.Scatter(
            x=merged_df["Date"],
            y=merged_df["LM893064105.Q"],
            mode="lines",
            name="LM893064105.Q",
            line=dict(color="red"),
        ),
        secondary_y=True,
    )

    # Update layout
    fig.update_layout(
        title="Area Plot of Series with Secondary Y-Axis for LM893064105.Q",
        xaxis_title="Date",
        yaxis_title="Series Values",
        yaxis2_title="LM893064105.Q",
    )

    # Show the plot
    fig.show()


if __name__ == "__main__":
    series_id = "NCBCEBQ027S"
    # graph_bokeh(series_id)
    # graph_ploty()

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
