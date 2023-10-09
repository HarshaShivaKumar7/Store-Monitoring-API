# business_hours.py

import csv

def load_business_hours_data(cursor):
    with open("./data/bussinesshours.csv", 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            cursor.execute(
                "INSERT INTO business_hours (store_id, day_of_week, start_time_local, end_time_local) VALUES (%s, %s, %s, %s)",
                (row['store_id'], int(row['day']), row['start_time_local'], row['end_time_local'])
            )
    cursor.connection.commit()
