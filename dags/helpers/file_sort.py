import os
import shutil
import glob
import re
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        #logging.FileHandler(r'./logfile.log')
    ]
)

# Define the function to sort files
def sort_files(**kwargs):
    folder_to_watch = "/data"  # Replace with the correct path
    logging.info(f"Monitoring folder: {folder_to_watch}")

    # Check if folder exists
    if not os.path.exists(folder_to_watch):
        logging.error(f"Folder does not exist: {folder_to_watch}")
        return

    # Log the current files in the folder
    files_in_directory = os.listdir(folder_to_watch)
    logging.debug(f"Files in directory before processing: {files_in_directory}")

    # Use glob to find files in the folder
    for file_path in glob.glob(os.path.join(folder_to_watch, "*")):
        file_name = os.path.basename(file_path)
        logging.info(f"Processing file: {file_name}")

        # Check if the path is a file
        if os.path.isfile(file_path):
            # Use regex to capture the base name
            match = re.match(r'(\w+)', file_name)
            if match:
                base_name = match.group(1)
                dest_dir = os.path.join(folder_to_watch, base_name)

                # Ensure the destination directory exists
                if not os.path.exists(dest_dir):
                    os.makedirs(dest_dir)
                    logging.info(f"Created directory: {dest_dir}")

                # Determine destination file path
                destination_file_path = os.path.join(dest_dir, file_name)

                # If the destination file already exists, append a timestamp
                if os.path.exists(destination_file_path):
                    # Get the current timestamp
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    # Create a new file name with the timestamp
                    base, ext = os.path.splitext(file_name)
                    new_file_name = f"{base}_{timestamp}{ext}"
                    destination_file_path = os.path.join(dest_dir, new_file_name)
                    logging.info(f"File already exists. Renaming to: {new_file_name}")

                # Move the file to the destination directory
                try:
                    shutil.move(file_path, destination_file_path)
                    logging.info(f"Moved file: {file_name} to {destination_file_path}")
                except Exception as e:
                    logging.error(f"Failed to move file: {file_name}. Error: {e}")
            else:
                logging.warning(f"Filename does not match expected pattern: {file_name}")
        else:
            logging.info(f"Skipping directory: {file_path}")

    # Log the updated files in the folder
    files_in_directory_after = os.listdir(folder_to_watch)
    logging.debug(f"Files in directory after processing: {files_in_directory_after}")