import pandas as pd
import psycopg2
from flask import Flask, jsonify, abort, request,send_file
import uuid
import os
import csv
from dotenv import load_dotenv

app = Flask(__name__)
reports_db = {}

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Function to connect to the database
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error: Unable to connect to the database: {e}")
        return None

def close_db_connection(conn):
    if conn:
        conn.close()

conn = connect_to_db()

# Function to create database tables
def create_tables(conn):
    try:
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS store_status (
                store_id VARCHAR,
                status VARCHAR,
                timestamp_utc TIMESTAMP
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS business_hours (
                store_id VARCHAR,
                day INTEGER,
                start_time_local TIME,
                end_time_local TIME
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS timezones (
                store_id VARCHAR,
                timezone_str VARCHAR
            )
        ''')
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error: Unable to create tables: {e}")

# Function to load data into the database
def load_data_into_db(conn):
    try:
        store_status_csv_path = "./data/status.csv"
        business_hours_csv_path = "./data/bussinesshours.csv"
        timezone_csv_path = "./data/timezones.csv"
        store_status_data = pd.read_csv(store_status_csv_path)
        business_hours_data = pd.read_csv(business_hours_csv_path, parse_dates=['start_time_local', 'end_time_local'])
        timezone_data = pd.read_csv(timezone_csv_path)
        business_hours_data['start_time_local'] = pd.to_datetime(business_hours_data['start_time_local'])
        business_hours_data['end_time_local'] = pd.to_datetime(business_hours_data['end_time_local'])
        business_hours_data.set_index('start_time_local', inplace=True)
        cur = conn.cursor()
        for _, row in store_status_data.iterrows():
            cur.execute(
                "INSERT INTO store_status (store_id, status, timestamp_utc) VALUES (%s, %s, %s)",
                (row['store_id'], row['status'], row['timestamp_utc'])
            )
        for _, row in business_hours_data.iterrows():
            cur.execute(
                "INSERT INTO business_hours (store_id, day, start_time_local, end_time_local) VALUES (%s, %s, %s, %s)",
                (row['store_id'], row['day'], row['start_time_local'], row['end_time_local'])
            )
        for _, row in timezone_data.iterrows():
            cur.execute(
                "INSERT INTO timezones (store_id, timezone_str) VALUES (%s, %s)",
                (row['store_id'], row['timezone_str'])
            )
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error: Unable to load data into the database: {e}")

conn.close()

def load_and_preprocess_data():
    store_status_data = pd.read_csv("./data/status.csv", parse_dates=["timestamp_utc"])
    business_hours_data = pd.read_csv("./data/bussinesshours.csv", parse_dates=["start_time_local", "end_time_local"])
    merged_data = pd.merge(store_status_data, business_hours_data, on='store_id')
    return merged_data

def calculate_uptime_downtime(merged_data):

    # Calculate uptime and downtime for each record
    merged_data['uptime'] = (merged_data['end_time_local'] - merged_data['start_time_local']).dt.total_seconds() / 60

    # If uptime is less than or equal to 0, set business_hours_downtime to 0
    merged_data['business_hours_downtime'] = merged_data['uptime']
    merged_data.loc[merged_data['uptime'] <= 0, 'business_hours_downtime'] = 0

    # If uptime exceeds 24 hours, set total_downtime to 0
    merged_data['total_downtime'] = 24 * 60 - merged_data['uptime']
    merged_data.loc[merged_data['uptime'] > 24 * 60, 'total_downtime'] = 24 * 60

    report_data = merged_data.groupby('store_id').agg({
        'uptime': 'sum',
        'business_hours_downtime': 'sum',
        'total_downtime': 'sum'
    }).reset_index()

    # Calculate uptime and downtime for the last hour

    current_timestamp = merged_data['timestamp_utc'].max()
    last_hour_timestamp = current_timestamp - pd.Timedelta(hours=1)
    last_hour_data = merged_data[(merged_data['timestamp_utc'] > last_hour_timestamp) & (merged_data['timestamp_utc'] <= current_timestamp)]
    uptime_last_hour = last_hour_data['uptime'].sum()
    downtime_last_hour = last_hour_data['total_downtime'].sum()

    # Calculate uptime and downtime for the last day

    last_day_timestamp = current_timestamp - pd.Timedelta(days=1)
    last_day_data = merged_data[(merged_data['timestamp_utc'] > last_day_timestamp) & (merged_data['timestamp_utc'] <= current_timestamp)]
    uptime_last_day = last_day_data['uptime'].sum() / 60
    downtime_last_day = last_day_data['total_downtime'].sum() / 60
    
    # Calculate uptime and downtime for the last week

    last_week_timestamp = current_timestamp - pd.Timedelta(days=7)
    last_week_data = merged_data[(merged_data['timestamp_utc'] > last_week_timestamp) & (merged_data['timestamp_utc'] <= current_timestamp)]
    uptime_last_week = last_week_data['uptime'].sum() / 60 / 24
    downtime_last_week = last_week_data['total_downtime'].sum() / 60 / 24

    # Add the calculated fields to the report data

    last_hour_timestamp = current_timestamp - pd.Timedelta(hours=1)
    last_hour_data = merged_data[(merged_data['timestamp_utc'] > last_hour_timestamp) & (merged_data['timestamp_utc'] <= current_timestamp)]
    uptime_last_hour = last_hour_data['uptime'].sum()
    downtime_last_hour = last_hour_data['total_downtime'].sum()

    last_day_timestamp = current_timestamp - pd.Timedelta(days=1)
    last_day_data = merged_data[(merged_data['timestamp_utc'] > last_day_timestamp) & (merged_data['timestamp_utc'] <= current_timestamp)]
    uptime_last_day = last_day_data['uptime'].sum() / 60
    downtime_last_day = last_day_data['total_downtime'].sum() / 60

    report_data['uptime_last_hour'] = uptime_last_hour
    report_data['downtime_last_hour'] = downtime_last_hour
    report_data['uptime_last_day'] = uptime_last_day
    report_data['downtime_last_day'] = downtime_last_day
    report_data['uptime_last_week'] = uptime_last_week
    report_data['downtime_last_week'] = downtime_last_week

    report_data = report_data[['store_id', 'business_hours_downtime', 'downtime_last_day', 'uptime_last_day', 'downtime_last_hour', 'uptime_last_hour', 'uptime_last_week', 'downtime_last_week']]

    return report_data

# Function to generate a CSV file
def generate_csv_file(report_data, file_name):
    # Directory to store CSV files
    csv_dir = "./csv_reports/"
    os.makedirs(csv_dir, exist_ok=True)

    file_path = os.path.join(csv_dir, file_name)

    # Write report data to a CSV file
    with open(file_path, 'w', newline='') as csvfile:
        fieldnames = ['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week', 
                      'downtime_last_hour', 'downtime_last_day', 'downtime_last_week']

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in report_data:
            # Adapt the data to match the field names
            adapted_row = {
                'store_id': row['store_id'],
                'uptime_last_hour': row['uptime_last_hour'],
                'uptime_last_day': row['uptime_last_day'],
                'uptime_last_week': row['uptime_last_week'],
                'downtime_last_hour': row['business_hours_downtime'],  # Adjust this based on your logic
                'downtime_last_day': row['downtime_last_day'],
                'downtime_last_week': row['downtime_last_week']
            }
            writer.writerow(adapted_row)

    return file_path

# API endpoint to trigger report generation
@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    merged_data = load_and_preprocess_data()
    report_data = calculate_uptime_downtime(merged_data)
    report_id = str(uuid.uuid4()).replace('-', '')
    reports_db[report_id] = report_data.to_dict(orient='records')
    return jsonify({"report_id": report_id})


# API endpoint to get the report
@app.route('/get_report/<report_id>', methods=['GET'])
def get_report(report_id):
    if report_id in reports_db:
        # Fetch the report data
        report_data = reports_db[report_id]

        # Check if report generation is complete
        if isinstance(report_data, list):
            # If report_data is a list, it means report generation is complete
            # Generate the CSV file and get its path
            file_path = generate_csv_file(report_data, f'{report_id}.csv')

            # Create the response JSON including both CSV file path and report data
            response = {
                'csv_file_path': file_path,
                'api_response_status': 'Complete',
                'report_data': report_data  # Include the entire report data
            }

            return jsonify(response)
        else:
            # Return "Running" status if report generation is not complete
            response = {
                'api_response_status': 'Running'
            }

            return jsonify(response)
    else:
        abort(404, description="Report ID not found")



if __name__ == '__main__':
    app.run(debug=True)
