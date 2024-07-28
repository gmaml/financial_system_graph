import xml.etree.ElementTree as ET
import pandas as pd
from tqdm import tqdm
from joblib import Parallel, delayed
import os
import logging
import argparse

# Set up logging
logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s"
)


def parse_z1data(z1_xml_file):
    # Check if the file exists
    if not os.path.exists(z1_xml_file):
        logging.error(f"File not found: {z1_xml_file}")
        return None, None

    # Define the namespaces
    namespaces = {
        "kf": "http://www.federalreserve.gov/structure/compact/Z1_Z1",
        "frb": "http://www.federalreserve.gov/structure/compact/common",
        "common": "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/common",
    }

    # Parse the XML file
    tree = ET.parse(z1_xml_file)
    root = tree.getroot()

    # Define column names and create an empty DataFrame
    column_names = ["Date", "Obs_value", "Series_name"]
    z1df = pd.DataFrame(columns=column_names)

    # Find all kf:Series elements
    series_elements = root.findall(".//kf:Series", namespaces)

    # Define a function to process each series element
    def process_series(series):
        series_attributes = series.attrib

        # Extract frb:Annotations data
        annotations_element = series.find("frb:Annotations", namespaces)
        if annotations_element is None:
            print("Error: Annotations element not found")
            return pd.DataFrame(columns=column_names), series_attributes

        for annotation in annotations_element.findall("common:Annotation", namespaces):
            annotation_type = annotation.find("common:AnnotationType", namespaces).text
            annotation_text = annotation.find("common:AnnotationText", namespaces).text
            series_attributes[annotation_type] = annotation_text

        # Iterate through frb:Obs elements within each kf:Series
        obs_value = []
        time_period = []
        for obs in series.findall("frb:Obs", namespaces):
            obs_value.append(obs.get("OBS_VALUE"))
            time_period.append(obs.get("TIME_PERIOD"))

        # Create a temporary DataFrame for the current series
        temp_dic = {"Date": time_period, "Obs_value": obs_value}
        tempdf = pd.DataFrame(temp_dic)
        tempdf["Series_name"] = series_attributes.get("SERIES_NAME", "Unknown")

        return tempdf, series_attributes

    # Use joblib to parallelize the processing with tqdm progress bar
    num_cores = -1  # Use all available cores
    results = Parallel(n_jobs=num_cores)(
        delayed(process_series)(series)
        for series in tqdm(series_elements, desc="Processing series")
    )

    # Separate the results into DataFrames and attributes
    dataframes, attributes = zip(*results)

    # Combine all the DataFrames into the main DataFrame
    z1df = pd.concat(dataframes, ignore_index=True)

    # Convert the list of attributes into a DataFrame
    series_df = pd.DataFrame(attributes)

    # Convert columns to appropriate data types
    z1df["Date"] = pd.to_datetime(z1df["Date"])
    z1df["Obs_value"] = z1df["Obs_value"].astype(float)
    z1df["Series_name"] = z1df["Series_name"].astype(str)

    return z1df, series_df


def main():
    parser = argparse.ArgumentParser(
        description="Process Z1 XML data and save as Parquet files."
    )
    parser.add_argument("xml_file", type=str, help="Path to the Z1 XML file")
    parser.add_argument(
        "save_path", type=str, help="Path to save the output Parquet files"
    )

    args = parser.parse_args()

    z1df, series_df = parse_z1data(args.xml_file)

    if z1df is not None and series_df is not None:
        z1df.to_parquet(os.path.join(args.save_path, "z1df.parquet"))
        series_df.to_parquet(os.path.join(args.save_path, "series_df.parquet"))
        print(f"Files saved successfully at {args.save_path}")
    else:
        print("Failed to process the XML file.")


if __name__ == "__main__":
    main()
