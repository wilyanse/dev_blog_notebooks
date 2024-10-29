import os
from dotenv import load_dotenv
from collections import defaultdict
import pandas as pd

load_dotenv()

# Specify the directory containing CSV files
csv_directory_path = os.getenv('data_path')
print(csv_directory_path)

# List all files in the directory
all_files = os.listdir(csv_directory_path)
specific_csv_files = [f for f in all_files if f.endswith('.csv')]

# Create a list of full file paths for the specific CSV files
specific_csv_paths = [os.path.join(csv_directory_path, f) for f in specific_csv_files]
print(specific_csv_paths)

# Initialize the dictionary with default lists
file_categories = defaultdict(list)

# Categorize filenames
types = ['biometrics', 'dailysummary', 'servings']
for filename in specific_csv_files:
    for type in types:
        if type in filename.lower():
            file_categories[type].append(os.path.join(csv_directory_path, filename))

print(file_categories['biometrics'])
dataframes = {}
# TODO: save categories to different pandas df
for category in file_categories:
    dataframes[category] = pd.DataFrame()
    for file in file_categories[category]:
        dataframes[category] = pd.concat([dataframes[category], pd.read_csv(file)])

print(dataframes['servings'])

# TODO: return pandas df in array