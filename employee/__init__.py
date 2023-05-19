import logging
import azure.functions as func
import pyodbc
import csv
import json
from datetime import datetime
from io import StringIO

# Read the secret.json file
with open('secrets.json') as secret_file:
    secret_data = json.load(secret_file)
 
# Access the user and password values
user = secret_data['user']
password = secret_data['password']

# Azure SQL Server connection string
conn_str = f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:alfredoglobant.database.windows.net,1433;Database=Employee;Uid={user};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryPassword"


def validate_transaction(transaction):
    try:
        # Validate required fields
        id, name, hire_datetime, department_id, job_id = transaction
        if not all([id, name, hire_datetime, department_id, job_id]):
            return False
        
        # Validate hire datetime format
        datetime.strptime(hire_datetime, "%Y-%m-%dT%H:%M:%SZ")
        
        return True
    except:
        return False

def process_batch_transactions(transactions):
    success_count = 0
    error_count = 0
    
    for transaction in transactions:
        if validate_transaction(transaction):
            # Insert the valid transaction into the corresponding table
            try:
                with pyodbc.connect(conn_str) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("INSERT INTO hired_employees (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)", transaction)
                
                success_count += 1
            except Exception as e:
                logging.error(f"Error inserting transaction: {e}")
                error_count += 1
        else:
            # Log the failed transaction
            logging.warning(f"Invalid transaction: {transaction}")
            error_count += 1
    
    return success_count, error_count

def get(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
    except ValueError:
        data = None

    id = data.get("id") if data else "*"

    try:
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                if id != "*":
                    cursor.execute("SELECT * FROM hired_employees WHERE id = ?", id)
                else:
                    cursor.execute("SELECT * FROM hired_employees")
                rows = cursor.fetchall()

        # Format the rows as needed (e.g., convert to JSON)
        response_data = [list(row) for row in rows]
        response_json = json.dumps(response_data)  # Convert the data to JSON format

        return func.HttpResponse(response_json, status_code=200, mimetype='application/json')
    except Exception as e:
        logging.error(f"Error processing GET request: {e}")
        return func.HttpResponse("Internal Server Error", status_code=500)


def put(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
        
        # Extract data from the request
        id = data["id"]
        name = data["name"]
        hire_datetime = data["hire_datetime"]
        department_id = data["department_id"]
        job_id = data["job_id"]
        
        # Update the corresponding resource with the extracted data
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE hired_employees SET name = ?, datetime = ?, department_id = ?, job_id = ? WHERE id = ?", name, hire_datetime, department_id, job_id, id)
        
        return func.HttpResponse("Resource updated successfully.", status_code=200)
    except Exception as e:
        logging.error(f"Error processing PUT request: {e}")
        return func.HttpResponse("Internal Server Error", status_code=500)


def delete(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
        
        # Extract data from the request
        id = data["id"]
        
        # Check if the resource exists before deleting
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM hired_employees WHERE id = ?", id)
                count = cursor.fetchone()[0]
        
        if count == 0:
            return func.HttpResponse("Resource does not exist.", status_code=404)
        
        # Delete the corresponding resource based on the extracted data
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM hired_employees WHERE id = ?", id)
        
        return func.HttpResponse("Resource deleted successfully.", status_code=200)
    except Exception as e:
        logging.error(f"Error processing DELETE request: {e}")
        return func.HttpResponse("Internal Server Error", status_code=500)


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        if req.method == "GET":
            return get(req)
        elif req.method == "PUT":
            return put(req)
        elif req.method == "DELETE":
            return delete(req)
        else:
            data = req.get_json()
            csv_data = '\n'.join(data["transactions"])  # Convert to a string with newline-separated rows
            csv_file = StringIO(csv_data)  # Create a file-like object
        
            transactions = list(csv.reader(csv_file))
        
            # Process batch transactions
            success_count, error_count = process_batch_transactions(transactions)
        
            return func.HttpResponse(f"Processed {success_count} transactions successfully. Encountered {error_count} errors.", status_code=200)
    except Exception as e:
        logging.error(f"Error processing batch transactions: {e}")
        return func.HttpResponse("Internal Server Error", status_code=500)
