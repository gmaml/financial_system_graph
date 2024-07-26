import os
import pandas as pd
import plotly.express as px
from fredapi import Fred


def main():
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
    fig.update_layout(title={"x": 0.5})

    # Show the plot
    fig.show()


if __name__ == "__main__":
    main()
