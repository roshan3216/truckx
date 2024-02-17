from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import time
import random

app = Flask(__name__)
# app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://roshan:roshan@localhost/truckx'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///sensor_data.db'

db = SQLAlchemy()
db.init_app(app)


class TemperatureData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sensor_id = db.Column(db.Integer, nullable=False)
    temperature = db.Column(db.Float, nullable=False)
    timestamp = db.Column(db.BigInteger, nullable=False)


class AggregateData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    # sensor_id = db.Column(db.Integer,nullable=False)
    sensor_id = db.Column(db.Integer,db.ForeignKey('temperature_data.sensor_id') ,nullable=False)
    avg_temperature = db.Column(db.Float, nullable=False)
    max_temperature = db.Column(db.Float, nullable=False)
    min_temperature = db.Column(db.Float, nullable=False)
    timestamp = db.Column(db.BigInteger, nullable=False)


def handle_error(message, status_code):
    response = {
        'error': message,
    }
    return jsonify(response), status_code

with app.app_context():
    # Create tables
    db.create_all()


def insert_temperature_data():
    sensor_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    n = 7200
    now = datetime.utcnow()
    # Ensure tables are created within Flask app context
    with app.app_context():
        for id in sensor_ids:
            timestamp_now = int(now.timestamp())
            i = 0
            temperature_objects_list = []
            start_time = time.time()
            while i <= n:
                temperature_objects_list.append(
                    TemperatureData(
                        sensor_id=id,
                        temperature=round(random.uniform(23, 50), 2),
                        timestamp=timestamp_now - i
                    )
                )
                i += 1
                if i % 100 == 0:
                    db.session.add_all(temperature_objects_list)
                    db.session.commit()

            print('Time taken to insert for one sensor id = ', id, ' and time = ', time.time() - start_time)

def aggregate_data():

    try: 
        with app.app_context():
            sensors = db.session.query(TemperatureData.sensor_id).distinct().all()
            now = datetime.utcnow()
            now_timestamp = int(now.timestamp())
            hour_ago = now - timedelta(hours=1)
            hour_ago_timestamp = int(hour_ago.timestamp())
            print(sensors , '[sensors]')

            for sensor in sensors:
                sensor_id = sensor[0]
                aggregated_data = db.session.query(
                    func.avg(TemperatureData.temperature).label('avg_temp'),
                    func.max(TemperatureData.temperature).label('max_temp'),
                    func.min(TemperatureData.temperature).label('min_temp')
                ).filter(
                    TemperatureData.sensor_id == sensor_id,
                    TemperatureData.timestamp >= hour_ago_timestamp
                    # TemperatureData.timestamp >= 1707995490
                ).first()

                print(aggregated_data, '[aggregate_data]')

                if aggregated_data:
                    new_aggregate = AggregateData(
                        sensor_id=sensor_id,
                        avg_temperature=aggregated_data.avg_temp,
                        max_temperature=aggregated_data.max_temp,
                        min_temperature=aggregated_data.min_temp,
                        timestamp=now_timestamp
                        # timestamp=1707999090
                    )
                    db.session.add(new_aggregate)
                    db.session.commit()

            return jsonify({'message': 'Aggregation completed successfully'}), 200    
    except Exception as e: 
        print ( str(e), '[error ing aggregate_data]')
        # scheduler.shutdown()
        return jsonify({'error': 'Internal Server Error', 'message': str(e)}), 500



@app.route('/temperature', methods=['POST'])
def add_temperature():
    try:
        data = request.get_json()
        keys_to_check = ['sensor_id', 'temperature', 'timestamp']
        missing_keys = [key for key in keys_to_check if key not in data]

        if missing_keys : 
            return handle_error('All keys not present', 400)
        

        sensor_id = data['sensor_id']
        temperature = data['temperature']
        timestamp = data['timestamp']
        print(data, sensor_id, temperature, timestamp, '[data, sensor_id, temperature, timestamp]-[add_temperature]')

        try:
            # conidering the timestamp to be in IST and converting it to utc datetime 
            ist_datetime = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            # getting utc datetime as IST is +5:30 from UTC
            utc_datetime = ist_datetime - timedelta(hours = 5.5) 
            utc_timestamp = utc_datetime.timestamp()
        except Exception as e : 
            return handle_error('Provide timestamp in "YYYY-MM-DD HH:MM:SS" format', 400)

        new_temperature = TemperatureData(
            sensor_id=sensor_id,
            temperature=temperature,
            # timestamp=datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S').timestamp()
            timestamp= utc_timestamp
        )
        db.session.add(new_temperature)
        db.session.commit()
        return jsonify({'message': f'Temperature data added successfully for sensor_id = {sensor_id}'}), 201
    except Exception as e:
        return handle_error(str(e), 500)
    

# @app.route('/temperature', methods = ['GET'])
# def get_temperature_data():
#     try:
#         data = TemperatureData.query.limit(100).all()
#         result = []
#         for row in data:
#             result.append({
#                 'id': row.id,
#                 'sensor_id': row.sensor_id,
#                 'temperature': row.temperature,
#                 'timestamp': row.timestamp,
#             })
#         return jsonify(result), 200
#     except Exception as e:
#         return handle_error(str(e), 500)


@app.route('/aggregate/<int:sensor_id>', methods=['GET'])
def get_aggregate_data(sensor_id):
    try:
        start_timestamp = request.args.get('start_timestamp')
        end_timestamp = request.args.get('end_timestamp')

        query = AggregateData.query.filter_by(sensor_id=sensor_id)

        if start_timestamp:
            try:
                ist_datetime = datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S')
                utc_datetime = ist_datetime - timedelta(hours = 5.5)
                start_utc_timestamp = utc_datetime.timestamp()
            except Exception as e: 
                return handle_error('Provide start timestamp in "YYYY-MM-DD HH:MM:SS" format', 400)
            query = query.filter(AggregateData.timestamp >= int(start_utc_timestamp))

        if end_timestamp:
            try:
                ist_datetime = datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S')
                utc_datetime = ist_datetime - timedelta(hours = 5.5)
                end_utc_timestamp = utc_datetime.timestamp()
            except Exception as e : 
                return handle_error('Provide end timestamp in "YYYY-MM-DD HH:MM:SS" format', 400)
            query = query.filter(AggregateData.timestamp <= int(end_utc_timestamp))

        data = query.order_by(AggregateData.timestamp.desc()).limit(1).all()

        if not data:
            return jsonify({'message': 'No aggregate data available for the specified sensor'}), 404

        result = []
        for entry in data:
            result.append({
                'sensor_id': sensor_id,
                'timestamp': entry.timestamp,
                'avg_temperature': entry.avg_temperature,
                'max_temperature': entry.max_temperature,
                'min_temperature': entry.min_temperature
            })
        return jsonify(result), 200
    except Exception as e:
        return handle_error(str(e), 500)



if __name__ == '__main__':

    # Scheduler for hourly aggregation
    # scheduler = BackgroundScheduler()
    # # Schedule the aggregation task every hour
    # scheduler.add_job(func=aggregate_data,trigger= 'interval', hours = 1)
    # scheduler.start()
    app.run(debug=True)
