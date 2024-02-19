# Truckx API

Truckx API is a Flask-based application that provides endpoints for aggregating and retrieving temperature data from sensors. The API uses Flask-Restx for creating a RESTful interface and SQLAlchemy for interacting with a MySQL database.

The api stores the temperature data recorded by the sensor. Currently there are only 10 senesors but can be increased by the user. <br/>

The records are stored in utc epoch timestamp in seconds. Whenever a user queries for an aggregate data the utc timestamps retrieeved from the database are converted into IST and served to the user. The temperature data from the sensors is expected to have the timestamp relative to IST in `YYYY-MM-DD HH:MM:SS` format which in turn is converted to utc timestamp before pushing to the database.


## Getting set up

Before getting started with the main tasks, you'll need to configure your
development environment and make sure the tests pass.

1. clone the repository
2. Navigate to the project-directory
3. create a python 3 virtual environment
4. install dependencies with pip
5. create a .env in the project root and configure the necessary environment variables
6. run the tests

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/roshan3216/truckx.git
   ```

2. cd truck-main

## Create a Python 3 virtual environment

Create a Python 3 venv to get started. If you have a preferred way of managing
venvs, then feel free to use that. If not, the following command will probably
be enough to get you going:

```
python3 -m venv .venv
.venv/Scripts/activate
```

## Install Dependencies with pip

Make use of the requirements.txt file to manage python dependencies. You can install it after activating the venv using
pip like so:

```
pip install -r requirements.txt
```
## Run the sever

To run the server and make use of the api it is wrapped up using swagger.


```
python main.py
```

## API Endpoints

### 1. Aggregate Data

- **Endpoint:** `/truckx/aggregate/<int:sensor_id>`
- **Method:** GET
- **Parameters:**
  - `sensor_id` (integer): The ID of the sensor for which to retrieve aggregated data.
  - `start_timestamp` (string, optional): Starting timestamp for the aggregate data query in "YYYY-MM-DD HH:MM:SS" format.
  - `end_timestamp` (string, optional): Ending timestamp for the aggregate data query in "YYYY-MM-DD HH:MM:SS" format.
- **Example:**

   ```bash
   curl -X GET "http://127.0.0.1:5000/truckx/aggregate/1?start_timestamp=2024-02-18%2000:00:00&end_timestamp=2024-02-20%2000:00:00"
   ```

### 2. Add Temperature Data

- **Endpoint:** `/truckx/temperature`
- **Method:** POST
- **Request Body (JSON):**
  - `sensor_id` (integer): ID of the sensor.
  - `temperature` (integer): Temperature reading of the sensor.
  - `timestamp` (string): Timestamp when the reading is recorded in "YYYY-MM-DD HH:MM:SS" format.
- **Example:**

  ```bash
  curl -X POST -H "Content-Type: application/json" -d '{
    "sensor_id": 1,
    "temperature": 25,
    "timestamp": "2022-03-01 12:30:00"
    }' http://127.0.0.1:5000/truckx/temperature
  ```

## Run the tests

I have used [`pytest`](https://pytest.org/) to run the tests:

```
pytest
```

