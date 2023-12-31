from flask import Flask, jsonify, abort
import pandas as pd
import psycopg2
import uuid
import os
import csv
from datetime import timedelta
from dotenv import load_dotenv
import time

app = Flask(__name__)
reports_db = {}

load_dotenv()

# Load environment variables
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Function to connect to the PostgreSQL database
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

# Function to load data into the PostgreSQL database
def load_data_into_db(conn):
    try:
        store_status_csv_path = "./data/status.csv"
        business_hours_csv_path = "./data/businesshours.csv"
        timezone_csv_path = "./data/timezones.csv"
        store_status_data = pd.read_csv(store_status_csv_path)
        business_hours_data = pd.read_csv(business_hours_csv_path, parse_dates=['start_time_local', 'end_time_local'])
        timezone_data = pd.read_csv(timezone_csv_path)

        with conn.cursor() as cur:
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

# Function to load and preprocess data from the PostgreSQL database
def load_and_preprocess_data():
    conn = connect_to_db()
    if conn is None:
        return None

    try:
        store_status_data = pd.read_sql("SELECT * FROM store_status", conn)
        business_hours_data = pd.read_sql("SELECT * FROM business_hours", conn)

        # Merge the data based on 'store_id'
        merged_data = pd.merge(store_status_data, business_hours_data, on='store_id')

        return merged_data
    except Exception as e:
        print(f"Error: Unable to load and preprocess data from the database: {e}")
        return None
    finally:
        if conn:
            conn.close()

def calculate_uptime_downtime(merged_data):
    try:
        # Convert 'start_time_local' and 'end_time_local' to timedelta type objects
        merged_data['start_time_local'] = merged_data['start_time_local'].apply(lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))
        merged_data['end_time_local'] = merged_data['end_time_local'].apply(lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))

        # Calculating the uptime and downtime for each record
        merged_data['uptime'] = (merged_data['end_time_local'] - merged_data['start_time_local']).dt.total_seconds() / 60

        # If uptime is less than or equal to 0, we will set business_hours_downtime to 0
        merged_data['business_hours_downtime'] = merged_data['uptime']
        merged_data.loc[merged_data['uptime'] <= 0, 'business_hours_downtime'] = 0

        # If uptime exceeds 24 hours, then we are setting total_downtime to 0
        merged_data['total_downtime'] = 24 * 60 - merged_data['uptime']
        merged_data.loc[merged_data['uptime'] > 24 * 60, 'total_downtime'] = 24 * 60

        report_data = merged_data.groupby('store_id').agg({
            'uptime': 'sum',
            'business_hours_downtime': 'sum',
            'total_downtime': 'sum'
        }).reset_index()

        # Calculate uptime and downtime 
        # last hour
        current_timestamp = merged_data['timestamp_utc'].max()
        last_hour_timestamp = current_timestamp - pd.Timedelta(hours=1)
        last_hour_data = merged_data[(merged_data['timestamp_utc'] > last_hour_timestamp) & (merged_data['timestamp_utc'] <= current_timestamp)]
        uptime_last_hour = last_hour_data['uptime'].sum()
        downtime_last_hour = last_hour_data['total_downtime'].sum()

        # last day
        last_day_timestamp = current_timestamp - pd.Timedelta(days=1)
        last_day_data = merged_data[(merged_data['timestamp_utc'] > last_day_timestamp) & (merged_data['timestamp_utc'] <= current_timestamp)]
        uptime_last_day = last_day_data['uptime'].sum() / 60
        downtime_last_day = last_day_data['total_downtime'].sum() / 60

        # last week
        last_week_timestamp = current_timestamp - pd.Timedelta(days=7)
        last_week_data = merged_data[(merged_data['timestamp_utc'] > last_week_timestamp) & (merged_data['timestamp_utc'] <= current_timestamp)]
        uptime_last_week = last_week_data['uptime'].sum() / 60 / 24
        downtime_last_week = last_week_data['total_downtime'].sum() / 60 / 24

        # Combining all the fields into the response report data
        report_data['uptime_last_hour'] = uptime_last_hour
        report_data['downtime_last_hour'] = downtime_last_hour
        report_data['uptime_last_day'] = uptime_last_day
        report_data['downtime_last_day'] = downtime_last_day
        report_data['uptime_last_week'] = uptime_last_week
        report_data['downtime_last_week'] = downtime_last_week

        report_data = report_data[['store_id', 'business_hours_downtime', 'downtime_last_day', 'uptime_last_day', 'downtime_last_hour', 'uptime_last_hour', 'uptime_last_week', 'downtime_last_week']]

        return report_data
    except Exception as e:
        print(f"Error: Unable to calculate uptime and downtime: {e}")
        return None

# Function to generate a CSV file
def generate_csv_file(report_data, file_name):
    try:
        csv_dir = "./csv_reports/"
        os.makedirs(csv_dir, exist_ok=True)
        file_path = os.path.join(csv_dir, file_name)

        # Writing the report data to a CSV file
        with open(file_path, 'w', newline='') as csvfile:
            fieldnames = ['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week',
                        'downtime_last_hour', 'downtime_last_day', 'downtime_last_week']

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in report_data:
                adapted_row = {
                    'store_id': row['store_id'],
                    'uptime_last_hour': row['uptime_last_hour'],
                    'uptime_last_day': row['uptime_last_day'],
                    'uptime_last_week': row['uptime_last_week'],
                    'downtime_last_hour': row['business_hours_downtime'],
                    'downtime_last_day': row['downtime_last_day'],
                    'downtime_last_week': row['downtime_last_week']
                }
                writer.writerow(adapted_row)

        return file_path
    except Exception as e:
        print(f"Error: Unable to generate CSV file: {e}")
        return None

# API endpoint to trigger report generation
@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    try:
        merged_data = load_and_preprocess_data()
        if merged_data is None:
            abort(500, description="Unable to load and preprocess data")

        report_data = calculate_uptime_downtime(merged_data)
        if report_data is None:
            abort(500, description="Unable to calculate uptime and downtime")

        report_id = str(uuid.uuid4()).replace('-', '')
        reports_db[report_id] = report_data.to_dict(orient='records')
        return jsonify({"report_id": report_id})
    except Exception as e:
        print(f"Error: Unable to trigger report: {e}")
        abort(500, description="Unable to trigger report")

# API endpoint to get the report
@app.route('/get_report/<report_id>', methods=['GET'])
def get_report(report_id):
    try:
        if report_id in reports_db:
            report_data = reports_db[report_id]

            if isinstance(report_data, list):
                file_path = generate_csv_file(report_data, f'{report_id}.csv')
                if file_path is None:
                    abort(500, description="Unable to generate CSV file")

                response = {
                    'csv_file_path': file_path,
                    'api_response_status': 'Complete',
                    'report_data': report_data
                }

                return jsonify(response)
            else:
                response = {
                    'api_response_status': 'Running'
                }

                return jsonify(response)
        else:
            abort(404, description="Report ID not found")
    except Exception as e:
        print(f"Error: Unable to get report: {e}")
        abort(500, description="Unable to get report")

if __name__ == '__main__':
    while True:
        try:
            app.run(debug=True)
        except Exception as e:
            print(f"Error: Unable to start the application: {e}")
            time.sleep(3)  
