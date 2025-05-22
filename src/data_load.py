import os
import json
from src.config import INPUT_PATH_DATA

def load_data():
    datasets_folder = INPUT_PATH_DATA  # Update with your actual folder path
    # List to store all JSON data
    file_name = []
    all_data = []

    for filename in os.listdir(datasets_folder):
        if filename.endswith(".json"):  # Filter for JSON files
            filepath = os.path.join(datasets_folder, filename)
            file_name.append(filename)

            with open(filepath, 'r') as f:
                data = json.load(f)
                # Extend the all_data list with the data from the current file
                all_data.extend(data)
                # Close the file
                f.close()
    return all_data, file_name
