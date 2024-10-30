import os
from dotenv import load_dotenv

from helpers.database_manager import DatabaseManager
from helpers.load import initialize_database, load_csv_to_postgres

load_dotenv()
dbManager = DatabaseManager(os.getenv('user'), os.getenv('dbname'), os.getenv('password'), os.getenv('host'), os.getenv('port'))
