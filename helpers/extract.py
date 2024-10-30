import os
from dotenv import load_dotenv
from collections import defaultdict
import pandas as pd

load_dotenv()

def convert_csv_to_dataframes(csv_directory_path):

    # List all files in the directory
    all_files = os.listdir(csv_directory_path)
    specific_csv_files = [f for f in all_files if f.endswith('.csv')]

    # Create a list of full file paths for the specific CSV files
    specific_csv_paths = [os.path.join(csv_directory_path, f) for f in specific_csv_files]
    print(specific_csv_paths)

    # Initialize the dictionary with default lists
    file_categories = defaultdict(list)

    # Categorize filenames
    types = ['biometrics', 'dailysummary', 'servings', 'workouts']
    for type in types:
        for filename in specific_csv_files:
            if type in filename.lower():
                file_categories[type].append(os.path.join(csv_directory_path, filename))

    dataframes = {}
    for category in file_categories:
        dataframes[category] = pd.DataFrame()
        for file in file_categories[category]:
            dataframes[category] = pd.concat([dataframes[category], pd.read_csv(file)]).reset_index(drop=True)

    return dataframes

csv_directory_path = os.getenv('data_path')
dataframes = convert_csv_to_dataframes(csv_directory_path)
print(dataframes)