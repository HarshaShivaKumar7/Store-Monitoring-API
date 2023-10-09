<<<<<<< HEAD
# Store Monitoring API

This repository contains the code for a Store Monitoring API that helps monitor the online status of restaurants during their business hours. It provides APIs to trigger report generation and retrieve the status report.

## Prerequisites
- Python 3.x
- PostgreSQL installed and running

## Setup
1. Clone the repository:
    ```bash
    git clone https://github.com/HarshaShivaKumar7/Store-Monitoring-API.git
    cd Store-Monitoring-API
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Configure the database:
    - Modify the database connection details in `app.py` in the `connect_to_db` function to match your PostgreSQL setup.

4. Run the application:
    ```bash
    python app.py
    ```

5. The API will be accessible at `http://localhost:5000`.

## API Endpoints

### 1. Trigger Report
- **Endpoint:** `/trigger_report`
- **Method:** POST
- **Description:** Triggers the generation of a store status report.
- **Response:**
  - JSON with the report ID.

### 2. Get Report
- **Endpoint:** `/get_report/<report_id>`
- **Method:** GET
- **Description:** Retrieves the store status report for the specified report ID.
- **Response:**
  - JSON containing the store status report.

## Usage
1. Trigger the report generation by making a POST request to `/trigger_report`.
2. Retrieve the report by making a GET request to `/get_report/<report_id>` using the report ID obtained from the trigger response.

## Contributors
- H S Harsha

=======
# Store-Monitoring-API
# Store-Monitoring-API
>>>>>>> 93cbc1bbc567f45627a92307b0f42f4f22065391
