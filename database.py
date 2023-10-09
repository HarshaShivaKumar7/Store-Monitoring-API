# database.py

import psycopg2

# Connect to the PostgreSQL database
def connect_to_db():
    conn = psycopg2.connect(
        dbname="loop_monitor_db",
        user="postgres",
        password="9633",
        host="localhost",
        port="5432"
    )
    return conn, conn.cursor()
