import pytest
from main import app, db

@pytest.fixture()
def client():
    app.config['TESTING'] = True
    # app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///sensor_data2.db'
    client = app.test_client()
    yield client

# def test_add_temperature(client):
#     payload = {
#         'sensor_id': 1,
#         'temperature': 25.5,
#         'timestamp': '2024-02-14 12:00:00'
#     }
#     response = client.post('/truckx/temperature', json=payload)
#     assert response.status_code == 201

#     assert response.json == {
#                                 "message": "Temperature data added successfully for sensor_id = 1"
#                                 }

def test_add_temperature_missing_keys(client):
    payload = {
        'temperature': 25.5,
        'timestamp': '2024-02-14 12:00:00'
    }
    response = client.post('/truckx/temperature', json=payload)
    assert response.status_code == 400

    assert response.json == {
        'error': 'All keys not present'
    }

def test_add_temperature_invalid_timestamp(client):
    payload = {
        'sensor_id': 1,
        'temperature': 25.5,
        'timestamp': '2024-02-16 29:32:22'
    }
    response = client.post('/truckx/temperature', json=payload)
    assert response.status_code == 400

    assert response.json == {
        'error': "Provide timestamp in 'YYYY-MM-DD HH:MM:SS' format"
    }

def test_get_aggregate_data(client):
    response = client.get('/truckx/aggregate/1')
    assert response.status_code == 200

    assert response.json == [
        {
            "sensor_id": 1,
            "avg_temperature": 36.1355,
            "max_temperature": 49.8,
            "min_temperature": 22.36,
            "timestamp": "2024-02-19 03:20:23"
        }
    ]
    

def test_get_aggregate_data_invalid_start_timestamp(client):
    response = client.get('/truckx/aggregate/1?start_timestamp=invalid_timestamp')
    assert response.status_code == 400

    assert response.json == {
        'error' : "Provide start timestamp in 'YYYY-MM-DD HH:MM:SS' format"
    }

def test_get_aggregate_data_invalid_end_timestamp(client):
    response = client.get('/truckx/aggregate/1?end_timestamp=invalid_timestamp')
    assert response.status_code == 400

    assert response.json == {
        'error' : "Provide end timestamp in 'YYYY-MM-DD HH:MM:SS' format"
    }


def test_get_aggregate_data_invalid_sensor(client):
    response = client.get('/truckx/aggregate/12')
    assert response.status_code == 404

    assert response.json == {
        'message' : "No aggregate data available for the specified sensor"
    }

