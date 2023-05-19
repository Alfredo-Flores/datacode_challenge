import logging
import pandas as pd
import pyodbc
import numpy as np
import azure.functions as func
import requests
import json
from io import StringIO


def retrieve_csv_data(blob_url: str):
    # Download the blob content as text 
    response = requests.get(blob_url)
    blob_data = response.text

    return blob_data

def clean_data(data: pd.DataFrame, column_names: list):
    # Clean null values in integer and float columns
    for column in column_names:
        if data[column].dtype in [np.int64, np.float64]:
            data[column].fillna(0, inplace=True)
        elif column == 'datetime':
            data[column].fillna(pd.NaT, inplace=True)
        else:
            data[column].fillna('', inplace=True)

    return data


def migrate_data_from_csv(csv_data: str, table_name: str, column_names: list):
    # Read CSV data into a DataFrame
    data = pd.read_csv(StringIO(csv_data))

    # Clean the data
    data = clean_data(data, column_names)

    logging.info(data)

    # Read the secret.json file
    with open('secrets.json') as secret_file:
        secret_data = json.load(secret_file)
     
    # Access the password value
    user = secret_data['user']
    password = secret_data['password']

    # Connect to the SQL database
    conn_str = f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:alfredoglobant.database.windows.net,1433;Database=Employee;Uid={user};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryPassword"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Truncate the table if it exists
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL TRUNCATE TABLE {table_name}")

    # Create the table if it doesn't exist
    create_table_sql = f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}') " \
                       f"CREATE TABLE {table_name} ({', '.join([f'{column} VARCHAR(255)' for column in column_names])})"
    cursor.execute(create_table_sql)
    conn.commit()

    # Prepare the insert statement
    placeholders = ', '.join(['?' for _ in column_names])
    sql = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
    params = [tuple(row[column] for column in column_names) for _, row in data.iterrows()]

    # Insert data into the SQL database table
    cursor.executemany(sql, params)
    conn.commit()

    logging.info('Done')

    # Close the database connection
    cursor.close()
    conn.close()


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Define the URLs for the CSV files
    department_csv_url = "https://globantcsv.blob.core.windows.net/csvtest/departments.csv"
    hired_employees_csv_url = "https://globantcsv.blob.core.windows.net/csvtest/hired_employees.csv"
    jobs_csv_url = "https://globantcsv.blob.core.windows.net/csvtest/jobs.csv"

    # Define column names for the tables
    department_column_names = ['id', 'department']
    hired_employees_column_names = ['id', 'name', 'datetime', 'department_id', 'job_id']
    jobs_column_names = ['id', 'job']

    # Retrieve CSV data from the URLs
    department_csv_data = retrieve_csv_data(department_csv_url)
    hired_employees_csv_data = retrieve_csv_data(hired_employees_csv_url)
    jobs_csv_data = retrieve_csv_data(jobs_csv_url)

    # Migrate data from CSV content to the SQL database
    migrate_data_from_csv(department_csv_data, 'departments', department_column_names)
    migrate_data_from_csv(hired_employees_csv_data, 'hired_employees', hired_employees_column_names)
    migrate_data_from_csv(jobs_csv_data, 'jobs', jobs_column_names)

    return func.HttpResponse("Data migration completed successfully.", status_code=200)
