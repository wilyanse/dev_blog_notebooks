import os
from dotenv import load_dotenv
from datetime import datetime
from database_manager import DatabaseManager
import pandas as pd

def upload_mock_data(dbManager, table_name):
    print("Upload mock data testing: ")

    # reading from CSV files
    df1 = pd.read_csv('mock_data.csv')
    df2 = pd.read_csv('sfoc_mcr_mock_data.csv')

    # converts CSV values from sfoc and mcr to list values
    df1['sfoc'] = [df2['sfoc'].tolist()] * len(df1)
    df1['mcr'] = [df2['mcr'].tolist()] * len(df1)

    # adds current time to timestamp
    df1['timestamp'] = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * len(df1)
    
    # calls insert dataframe
    dbManager.insert_dataframe(table_name, df1)
    print('In database \n')

def insert_mock_data(dbManager, table_name):
    print("Insert mock data testing:")

    # gets the field names as a mutable object
    fields = tuple(dbManager.get_fields(table_name))
    date = datetime.now()
    
    # inputs new entry values
    new_entry = ['0', date, [0, 0], [0, 0, 0], 8, 9, 10, 11, 12, 13, 14]
    dbManager.create_entry(table_name, fields, new_entry)
    print('Inserted')

    fields, entries = dbManager.get_value(table_name, ('ship_id',), ('0',))
    print(entries[0])
    print('\n')

def update_mock_data(dbManager, table_name):
    print("Update mock data testing: ")

    # initial reading of the details of dbManager
    fields, entries = dbManager.get_value(table_name, ('ship_id',), ('0',))
    print(entries[0])

    # updating the table using existing fields and new values
    fields = tuple(dbManager.get_fields(table_name))
    new_entry = ['0', datetime.now(), [0, 0], [0, 0, 0], 1, 2, 3, 4, 5, 6, 7]
    dbManager.update_value(table_name, fields, new_entry)

    # updated reading of the first element of dbManager
    fields, entries = dbManager.get_value(table_name, ('ship_id',), ('0',))
    print(entries[0])
    print("\n")

def delete_mock_data(dbManager, table_name):
    print("Delete mock data testing: ")

    # initial reading of the table
    fields, entries = dbManager.get_value(table_name, ('ship_id',), ('0',))
    print(entries[0])

    # deletes the input ship_id from insert_mock_data
    fields = ('ship_id',)
    values = ('0',)
    dbManager.delete_value(table_name, fields, values)

    # new reading of database table
    fields, entries = dbManager.get_value(table_name, ('ship_id',), ('0',))
    if len(entries) != 0:
        print(entries[0])
    else:
        print('No entry exists.')

load_dotenv()
dbManager = DatabaseManager(os.getenv('user'), os.getenv('dbname'), os.getenv('password'), os.getenv('host'), os.getenv('port'))
upload_mock_data(dbManager, 'parameters')
insert_mock_data(dbManager, 'parameters')
update_mock_data(dbManager, 'parameters')
delete_mock_data(dbManager, 'parameters')
dbManager.conn.close()