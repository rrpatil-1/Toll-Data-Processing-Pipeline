import pandas as pd
import argparse
import os
from Utilies import ROOT_DIR


def extract_trips(input_file, output_dir):
    # Read the Parquet file

    df = pd.read_parquet(input_file)
    df = df.convert_dtypes()

    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])


    # Sort the dataframe by unit and timestamp
    df.sort_values(by=['unit', 'timestamp'], inplace=True)

    # Initialize variables to track trip information
    current_unit = None
    trip_number = 0
    trip_data = []

    # Function to save trip data to CSV
    def save_trip_data(unit, trip_number, trip_data):
        trip_df = pd.DataFrame(trip_data, columns=['latitude', 'longitude', 'timestamp'])
        trip_csv_name = f"{unit}_{trip_number}.csv"
        trip_csv_path = os.path.join(output_dir, trip_csv_name)

        trip_df.to_csv(trip_csv_path, index=False)
        print(f"Saved trip data to {trip_csv_path}")

    # Iterate through the rows of the dataframe
    for index, row in df.iterrows():
        unit = row['unit']
        timestamp = row['timestamp']

        if current_unit is None or current_unit != unit:
            # Start a new trip
            if current_unit is not None:
                save_trip_data(current_unit, trip_number, trip_data)
                trip_number += 1
                trip_data = []

            current_unit = unit

        # Check for time difference to identify new trips gt = 7 new trip start
        if index > 0:
            time_diff = (timestamp - df.at[index - 1, 'timestamp']).total_seconds() / 3600
            if time_diff > 7:
                save_trip_data(current_unit, trip_number, trip_data)
                trip_number += 1
                trip_data = []

        # Append data to the current trip
        # change the timestamp to proper format
        trip_data.append([row['latitude'], row['longitude'], timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")])

    # Save the last trip data
    if current_unit is not None:
        save_trip_data(current_unit, trip_number, trip_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process GPS data and extract trips.')
    parser.add_argument('--to_process', type=str, required=True,  help='Path to the Parquet file to be processed.')
    parser.add_argument('--output_dir', type=str, required=True, help='Folder to store the resulting CSV files.')

    """#INFO:: comment the above 👆 2 lines and uncomment below 👇 2 lines to debug the code"""
    # parser.add_argument('--to_process', type=str,help='Path to the Parquet file to be processed.')
    # parser.add_argument('--output_dir', type=str, help='Folder to store the resulting CSV files.')

    args = parser.parse_args()

    """#INFO:: uncomment below 👇 two line to debug the code"""
    # args.to_process='/evaluation_data/input/raw_data.parquet'
    # args.output_dir = /evaluation_data/output/process1'

    args.to_process=str(ROOT_DIR) + args.to_process
    args.output_dir = str(ROOT_DIR) + args.output_dir
    if not os.path.exists(args.to_process):
        raise Exception(f'file not found:{args.to_process}')
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    extract_trips(args.to_process, args.output_dir)
