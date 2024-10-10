import requests
from dotenv import load_dotenv
import os
import pandas as pd

base_url = 'https://api.hevyapp.com/v1/workouts?page={}&pageSize=10'
headers = {'api-key' : os.getenv('hevy_api_key')}

# Function to fetch data for a specific page
def fetch_workout_data(page_num):
    url = base_url.format(str(page_num))
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data on page {page_num}. Status code: {response.status_code}")


# Function to process workout data and convert it to a CSV
def process_and_save_to_csv(workout_data, filename='workouts.csv'):
    workout_list = []

    # Loop through all workouts on all pages
    for workout in workout_data:
        workout_id = workout['id']
        title = workout['title']
        start_time = workout['start_time']
        end_time = workout['end_time']
        
        # Loop through exercises in the workout
        for exercise in workout['exercises']:
            exercise_title = exercise['title']
            for workout_set in exercise['sets']:
                workout_list.append({
                    'Workout ID': workout_id,
                    'Title': title,
                    'Start Time': start_time,
                    'End Time': end_time,
                    'Exercise Title': exercise_title,
                    'Set Type': workout_set['set_type'],
                    'Reps': workout_set['reps'],
                    'Weight (kg)': workout_set['weight_kg'],
                    'Distance (meters)': workout_set['distance_meters'],
                    'Duration (seconds)': workout_set['duration_seconds'],
                    'RPE': workout_set['rpe']
                })
    
    # Create a DataFrame and save to CSV
    df = pd.DataFrame(workout_list)
    df.to_csv(filename, index=False)

# Main function to fetch all pages
def fetch_all_workouts():
    # First, fetch page 1 to get the total number of pages (page_count)
    first_page_data = fetch_workout_data(1)
    total_pages = first_page_data['page_count']
    
    # Store all workouts
    all_workouts = first_page_data['workouts']
    
    # Fetch data for all remaining pages
    for page_num in range(2, total_pages + 1):
        page_data = fetch_workout_data(page_num)
        all_workouts.extend(page_data['workouts'])
    filename = os.getenv('hevy_data_location')
    print(filename)
    # Process and save all workouts to CSV
    #process_and_save_to_csv(all_workouts, filename=FILENAME)

# Run the function to fetch and save all workout data
filename = os.getenv('hevy_data_location')
print(filename)