from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from flask_apscheduler import APScheduler
from datetime import datetime, timedelta

# app = Flask(__name__)
# app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://roshan:roshan@localhost:3306/truckx'
# db = SQLAlchemy(app)


db = SQLAlchemy()
def aggregate_data():
    sensors = db.session.query(TemperatureData.sensor_id).distinct().all()
    print(sensors , '[sensors]-[aggregate_data]')
    hour_ago = datetime.now() - timedelta(hours=1)

    for sensor in sensors:
        sensor_id = sensor[0]
        hour_ago_timestamp = int(hour_ago.timestamp())

        aggregated_data = db.session.query(
            func.avg(TemperatureData.temperature).label('avg_temp'),
            func.max(TemperatureData.temperature).label('max_temp'),
            func.min(TemperatureData.temperature).label('min_temp')
        ).filter(
            TemperatureData.sensor_id == sensor_id,
            TemperatureData.timestamp >= hour_ago_timestamp
        ).first()

        if aggregated_data:
            new_aggregate = AggregateData(
                sensor_id=sensor_id,
                avg_temperature=aggregated_data.avg_temp,
                max_temperature=aggregated_data.max_temp,
                min_temperature=aggregated_data.min_temp,
                timestamp=datetime.now()
            )
            db.session.add(new_aggregate)
            db.session.commit()

class Config:
    """App configuration."""

    JOBS = [{"id": "job1", "func": aggregate_data, "trigger": "interval", "seconds": 2}]

    SCHEDULER_JOBSTORES = {
        "default": SQLAlchemyJobStore(url="mysql://roshan:roshan@localhost:3306/truckx")
    }

    SCHEDULER_API_ENABLED = True

if __name__ == '__main__':
    app = Flask (__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://roshan:roshan@localhost:3306/truckx'
    app.config.from_object(Config())
    db.app = app
    db.init_app(app)
    

    scheduler = APScheduler()
    scheduler.init_app(app)
    scheduler.start()
    
    
    app.run(debug=True)

class TemperatureData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sensor_id = db.Column(db.String(50), nullable=False)
    temperature = db.Column(db.Float, nullable=False)
    timestamp = db.Column(db.BigInteger, nullable=False)

class AggregateData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sensor_id = db.Column(db.String(50), nullable=False)
    avg_temperature = db.Column(db.Float, nullable=False)
    max_temperature = db.Column(db.Float, nullable=False)
    min_temperature = db.Column(db.Float, nullable=False)
    timestamp = db.Column(db.BigInteger, nullable=False)




with app.app_context():
    # Create tables
    db.create_all()
    # Scheduler for hourly aggregation
    scheduler = BackgroundScheduler()

    
    # Schedule the aggregation task every hour
    scheduler.__init__(app)
    scheduler.start()
    scheduler.add_job(aggregate_data, 'interval', seconds=5)
    print(scheduler, '[scheduler]')


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


@app.route('/temperature', methods = ['GET'])
def get_temperature_data():
    data = TemperatureData.query.all()

    print(data, '[data]')

    return jsonify(data)

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





    
    # print('yyy', '[yyy]')
