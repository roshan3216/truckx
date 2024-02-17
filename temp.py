from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://roshan:roshan@localhost/truckx'
db = SQLAlchemy(app)

class TemperatureData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sensor_id = db.Column(db.Integer, nullable=False)
    temperature = db.Column(db.Float, nullable=False)
    timestamp = db.Column(db.BigInteger, nullable=False)

class AggregateData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sensor_id = db.Column(db.Integer, nullable=False)
    avg_temperature = db.Column(db.Float, nullable=False)
    max_temperature = db.Column(db.Float, nullable=False)
    min_temperature = db.Column(db.Float, nullable=False)
    timestamp = db.Column(db.BigInteger, nullable=False)

with app.app_context():
    # Create tables
    db.create_all()



def aggregate_data():
    with app.app_context():
        sensors = db.session.query(TemperatureData.sensor_id).distinct().all()
        hour_ago = datetime.now() - timedelta(hours=1)
        hour_ago_timestamp = hour_ago.timestamp()
        now_timestamp = datetime.now().timestamp()
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
            breakpoint

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



@app.route('/temperature', methods=['POST'])
def add_temperature():
    data = request.get_json()

    new_temperature = TemperatureData(
        sensor_id=data['sensor_id'],
        temperature=data['temperature'],
        timestamp=datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S')
    )

    db.session.add(new_temperature)
    db.session.commit()

    return jsonify({'message': 'Temperature data added successfully'}), 201

@app.route('/aggregate/<sensor_id>', methods=['GET'])
def get_aggregate_data(sensor_id):
    data = AggregateData.query.filter_by(sensor_id=sensor_id).all()

    if not data:
        return jsonify({'message': 'No aggregate data available for the specified sensor'}), 404

    result = []
    for entry in data:
        result.append({
            'timestamp': entry.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'avg_temperature': entry.avg_temperature,
            'max_temperature': entry.max_temperature,
            'min_temperature': entry.min_temperature
        })

    return jsonify(result), 200

if __name__ == '__main__':
    # Scheduler for hourly aggregation
    scheduler = BackgroundScheduler()
    # Schedule the aggregation task every hour
    scheduler.add_job(func=aggregate_data,trigger= 'interval', seconds = 10)
    scheduler.start()
    scheduler.shutdown()
    app.run(debug=True)
