import requests
from dotenv import load_dotenv
import os

url = 'https://api.hevyapp.com/v1/workouts?page=1&pageSize=10'
headers = {'api-key' : os.getenv('hevy_api_key')}
response = requests.get(url, headers=headers)
print(response.text)