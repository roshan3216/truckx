# Truckx
The api stores the temperature data recorded by the sensor. Currently there are only 10 senesors but can be increased by the user. <br/>

MySQL database is used to store the sensor data and the instances of the records are stored in utc epoch timestamp in seconds. Whenever a user queries for an aggregate data the utc timestamps retrieeved from the database are converted into IST and served to the user. The temperature data from the sensors is expected to have the timestamp relative to IST in `YYYY-MM-DD HH:MM:SS` format which in turn is converted to utc timestamp before pushing to the database.


## Getting set up

Before getting started with the main tasks, you'll need to configure your
development environment and make sure the tests pass.

1. create a python 3 virtual environment
2. install dependencies with pip
3. run the tests

## Create a Python 3 virtual environment

Create a Python 3 venv to get started. If you have a preferred way of managing
venvs, then feel free to use that. If not, the following command will probably
be enough to get you going:

```For windows
python3 -m venv .venv
.venv/bin/activate
```

```For mac/linux
python3 -m venv .venv
source .venv/bin/activate
```

## Install Dependencies with pip

Make use of the requirements.txt file to manage python dependencies. You can install it after activating the venv using
pip like so:

```
pip install -r requirements.txt
```

## Run the tests

I have used [`pytest`](https://pytest.org/) to run the tests:

```
pytest
```
Covered the possible test cases to identify the edge cases too.

## Run the sever

To run the server and make use of the api it is wrapped up using swagger.


```
python main.py
```
The following URLs are supported by the API:

- `GET /truckx/aggregate/:id/` - retrieve the aggregate values for the sensor with ID `:id`
- `GET /truckx/aggregate/:id/?start_timestamp=&end_timestamp=` - retrieve the aggregate values for the sensor with ID `:id` having timestamps between `start_timestamp` and `end_timestamp`
- `POST /truckx/temperature/` - post the temperature reading of the sensor

