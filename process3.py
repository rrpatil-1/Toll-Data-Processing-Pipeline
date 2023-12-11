import os
import json
import csv
import argparse
from Utilies import ROOT_DIR


def process_json_folder(json_folder, output_file):
    # Initialize CSV writer

    with open(output_file, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write CSV header
        csv_writer.writerow(['unit', 'trip_id', 'toll_loc_id_start', 'toll_loc_id_end', 'toll_loc_name_start',
                             'toll_loc_name_end', 'toll_system_type', 'entry_time', 'exit_time',
                             'tag_cost', 'cash_cost', 'license_plate_cost'])

        # Process each JSON file
        for file_name in os.listdir(json_folder):
            if file_name.endswith('.json'):
                file_path = os.path.join(json_folder, file_name)

                process_json_file(file_path, csv_writer, file_name)

def process_json_file(file_path, csv_writer, trip_id):
    with open(file_path, 'r') as json_file:
        try:
            json_data = json.load(json_file)
            process_trip_data(json_data, csv_writer, trip_id)
        except json.JSONDecodeError as e:
            print(f"Error processing {file_path}: {e}")


def process_trip_data(json_data, csv_writer, trip_id):
    unit,trip_id = trip_id.split('_')[0],'_'.join(trip_id.split('_')[:2])

    if json_data['route']['hasTolls']:

        # todo :: unit = json_data['summary']['share']['uuid']
        #  uuid is unique identification number for each txn on vehicle
        #  for Unique identification number for the vehicle we could use this


        for toll in json_data['route']['tolls']:

            if toll.get('start'):
                toll_loc_id_start= toll['start']['id']
                toll_loc_id_end= toll['end']['id']
                toll_loc_name_start= toll['start']['name']
                toll_loc_name_end= toll['end']['name']
                toll_system_type= toll['type']
                entry_time= toll['start']['arrival']['time']
                exit_time= toll['end']['arrival']['time']
                tag_cost= toll['tagCost']
                cash_cost= toll['cashCost']
                license_plate_cost= toll['licensePlateCost']

            else:
                toll_loc_id_start= toll['id']
                toll_loc_id_end= ''
                toll_loc_name_start= toll['name']
                toll_loc_name_end= ''
                toll_system_type= toll['type']
                entry_time= toll['arrival']['time']
                exit_time= ''
                tag_cost= toll['tagCost']
                cash_cost= toll['cashCost']
                license_plate_cost= toll['licensePlateCost']

            csv_writer.writerow([unit,trip_id,toll_loc_id_start,toll_loc_id_end,toll_loc_name_start,toll_loc_name_end,toll_system_type,
                                 entry_time,exit_time, tag_cost,cash_cost,license_plate_cost])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract toll information from JSON files.')
    parser.add_argument('--to_process', type=str, required=True, help='Path to the JSON responses folder.')
    parser.add_argument('--output_dir', type=str, required=True,help='Folder to store the final transformed_data.csv.')

    """#INFO:: comment the above 👆 2 lines and uncomment below 👇 2 lines to debug the code"""
    # parser.add_argument('--to_process', type=str,help='Path to the Parquet file to be processed.')
    # parser.add_argument('--output_dir', type=str, help='Folder to store the resulting CSV files.')

    args = parser.parse_args()

    """#INFO:: uncomment below 👇 two line to debug the code"""
    # args.to_process = '/evaluation_data/output/process1'
    # args.output_dir = '/evaluation_data/output/process2'

    args.to_process = str(ROOT_DIR) + args.to_process
    args.output_dir = str(ROOT_DIR) + args.output_dir

    if not os.path.exists(args.to_process):
        raise Exception(f'folder not found:{args.to_process}')

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    output_file = os.path.join(args.output_dir, 'transformed_data.csv')

    process_json_folder(args.to_process, output_file)

    print(f"Processed toll information saved to {output_file}")
