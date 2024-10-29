import os
from dotenv import load_dotenv
from collections import defaultdict

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
for filename in specific_csv_files:
    if 'biometrics' in filename.lower():
        file_categories['biometrics'].append(os.path.join(csv_directory_path, filename))
    elif 'dailysummary' in filename.lower():
        file_categories['dailysummary'].append(os.path.join(csv_directory_path, filename))
    elif 'servings' in filename.lower():
        file_categories['servings'].append(os.path.join(csv_directory_path, filename))

print(file_categories)