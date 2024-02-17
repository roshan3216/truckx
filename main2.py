from flask import Flask, request, jsonify
from flask_restx import Api, Resource, reqparse, fields
from werkzeug.exceptions import BadRequest, InternalServerError
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import time
import random

app = Flask(__name__)
# app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://roshan:roshan@localhost/truckx'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///sensor_data2.db'

db = SQLAlchemy()
db.init_app(app)
api = Api(app,version='1.0', title='Truckx API',description='An aggregation and temperature creation API',)


ns = api.namespace('truckx', desctription = 'Aggregate and create API')

parser = reqparse.RequestParser()
parser.add_argument('start_timestamp', type=str, help='starting timestamp for aggregate data query in "YYYY-MM-DD HH:MM:SS" format')
parser.add_argument('end_timestamp', type=str, help='ending timestamp for aggregate data query in "YYYY-MM-DD HH:MM:SS" format')


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
    return response, status_code

with app.app_context():
    # Create tables
    db.create_all()




@ns.route('/aggregate/<int:sensor_id>')
class Aggregate(Resource) : 

    @ns.doc(parser = parser)
    def get(self, sensor_id):
        try:
            args = parser.parse_args()
            start_timestamp = args.get('start_timestamp')
            end_timestamp = args.get('end_timestamp')

            print(sensor_id, start_timestamp, end_timestamp, '[sensor_id, start_timestamp, end_timestamp]-[Aggregate]')

            query = AggregateData.query.filter_by(sensor_id=sensor_id)

            if start_timestamp:
                try:
                    ist_datetime = datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S')
                    utc_datetime = ist_datetime - timedelta(hours = 5.5)
                    start_utc_timestamp = utc_datetime.timestamp()
                except Exception as e:
                    print(e, '[e]')
                    # raise BadRequest('Provide start timestamp in "YYYY-MM-DD HH:MM:SS" format')
                    return handle_error("Provide start timestamp in 'YYYY-MM-DD HH:MM:SS' format", 400)
                query = query.filter(AggregateData.timestamp >= int(start_utc_timestamp))

            if end_timestamp:
                try:
                    ist_datetime = datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S')
                    utc_datetime = ist_datetime - timedelta(hours = 5.5)
                    end_utc_timestamp = utc_datetime.timestamp()
                except Exception as e : 
                    # raise BadRequest('Provide start timestamp in "YYYY-MM-DD HH:MM:SS" format')
                    return handle_error("Provide end timestamp in 'YYYY-MM-DD HH:MM:SS' format", 400)
                query = query.filter(AggregateData.timestamp <= int(end_utc_timestamp))

            data = query.order_by(AggregateData.timestamp.desc()).limit(1).all()

            print(data, '[data]-[Aggregate]')
            if not data:
                # raise BadRequest('No aggregate data available for the specified sensor')
                return {'message': 'No aggregate data available for the specified sensor'}, 404

            result = []
            for entry in data:
                timestamp = entry.timestamp
                utc_datetime = datetime.fromtimestamp(timestamp)
                ist_datetime = utc_datetime + timedelta(hours = 5.5)

                result.append({
                    'sensor_id': sensor_id,
                    'timestamp': ist_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                    'avg_temperature': entry.avg_temperature,
                    'max_temperature': entry.max_temperature,
                    'min_temperature': entry.min_temperature
                })
            return result, 200
        except Exception as e:
            print(str(e), '[error Aggregate]')
            raise InternalServerError('Something Went Wrong')


temperature_model = ns.model('temperature',{
    'sensor_id': fields.Integer(description = 'Id of the sensor'),
    'temperature': fields.Integer(description= 'Temperature reading of the sensor'),
    'timestamp': fields.String(description = 'Timestamp when the reading is recorded in "YYYY-MM-DD HH:MM:SS" format')
})

@ns.route('/temperature')
class Temperature(Resource) :

    @ns.expect(temperature_model)
    def post(self):
        try:
            data = request.get_json()
            print(data, '[data]-[add_temperature]')
            keys_to_check = ['sensor_id', 'temperature', 'timestamp']
            missing_keys = [key for key in keys_to_check if key not in data]

            if missing_keys : 
                # raise BadRequest('Provide start timestamp in "YYYY-MM-DD HH:MM:SS" format')
                return handle_error('All keys not present', 400)
            

            sensor_id = data['sensor_id']
            temperature = data['temperature']
            timestamp = data['timestamp']
            print(sensor_id, temperature, timestamp, '[sensor_id, temperature, timestamp]-[add_temperature]')

            try:
                # conidering the timestamp to be in IST and converting it to utc datetime 
                ist_datetime = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                # getting utc datetime as IST is +5:30 from UTC
                utc_datetime = ist_datetime - timedelta(hours = 5.5) 
                utc_timestamp = utc_datetime.timestamp()
            except Exception as e : 
                # return BadRequest('Provide start timestamp in "YYYY-MM-DD HH:MM:SS" format')
                return handle_error("Provide timestamp in 'YYYY-MM-DD HH:MM:SS' format", 400)

            new_temperature = TemperatureData(
                sensor_id=sensor_id,
                temperature=temperature,
                # timestamp=datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S').timestamp()
                timestamp= utc_timestamp
            )
            db.session.add(new_temperature)
            db.session.commit()
            return {'message': f'Temperature data added successfully for sensor_id = {sensor_id}'}, 201
        except Exception as e:
            print(str(e), '[error in Temperature class]')
            raise InternalServerError('Something Went Wrong')
            return handle_error({'error': 'Internal Server Error','message': 'Something went wrong'}, 500)



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
                ).order_by(TemperatureData.timestamp.desc()).first()

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
        return jsonify({'message': str(e)}), 500



if __name__ == '__main__':
    
    # insert_temperature_data()
    # Scheduler for hourly aggregation
    # scheduler = BackgroundScheduler()
    # # Schedule the aggregation task every hour
    # scheduler.add_job(func=aggregate_data,trigger= 'interval', seconds = 10)
    # scheduler.start()
    app.run(debug=True)
