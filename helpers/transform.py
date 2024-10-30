import re
import pandas
# Function to transform a single column name
def transform_column_name(col_name):
    col_name = col_name.lower()
    col_name = re.sub(r' ', '_', col_name)
    col_name = re.sub(r'[^a-z0-9_]', '', col_name)
    if not re.match(r'^[a-z_]', col_name):
        col_name = f'_{col_name}'
    col_name = col_name[:128]
    return col_name

def transform_data(df):
    new_column_names = [transform_column_name(col) for col in df.columns]
    df.columns = new_column_names
    return df