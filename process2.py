import os
import requests
import argparse
from dotenv import load_dotenv
import json
from Utilies import ROOT_DIR

load_dotenv()

def upload_to_tollguru(file_path, output_dir):
    api_url = os.getenv('TOLLGURU_API_URL')
    api_key = os.getenv('TOLLGURU_API_KEY')

    if not api_url or not api_key:
        raise ValueError("TollGuru API key or URL not provided. Make sure to set TOLLGURU_API_URL and TOLLGURU_API_KEY in your .env file.")

    # url = f'{api_url}/toll/v2/gps-tracks-csv-upload?mapProvider=osrm&vehicleType=5AxlesTruck'
    headers = {'x-api-key': api_key, 'Content-Type': 'text/csv'}

    with open(file_path, 'rb') as file:
        response = requests.post(api_url, data=file, headers=headers)

    if response.status_code == 200:
        json_response = response.json()
        output_file_path = os.path.join(output_dir, os.path.basename(file_path).replace('.csv', '_response.json'))

        with open(output_file_path, 'w') as output_file:
            # data = json.dumps(json_response).replace('null', '""')
            # output_file.write(data)
            json.dump(json_response, output_file)

        print(f"Saved JSON response to {output_file_path}")
    else:
        print(f"Failed to upload {file_path}. Status code: {response.status_code}, Error: {response.text}")

def process_csv_folder(csv_folder, output_dir):
    for file_name in os.listdir(csv_folder):
        if file_name.endswith('.csv'):
            file_path = os.path.join(csv_folder, file_name)
            upload_to_tollguru(file_path, output_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload GPS tracks to TollGuru API.')
    parser.add_argument('--to_process', type=str, required=True, help='Path to the CSV folder.')
    parser.add_argument('--output_dir', type=str, required=True, help='Folder to store the resulting JSON files.')

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
        raise Exception(f'file not found:{args.to_process}')

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    process_csv_folder(args.to_process, args.output_dir)
