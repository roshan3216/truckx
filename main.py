from flask import Flask, request, jsonify
from flask_restx import Api, Resource, reqparse, fields
from flask_sqlalchemy import SQLAlchemy
from dotenv  import load_dotenv
from sqlalchemy import func, create_engine, text
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import os
from urllib.parse import quote

load_dotenv()

db_host = os.getenv('db_host')
# db_port = int(os.getenv('db_port'))
db_name = os.getenv('db_name')
db_username = quote(os.getenv('db_username'), safe='')
db_password = os.getenv('db_password')

app = Flask(__name__)
db_config = f'mysql://{db_username}:{db_password}@{db_host}/{db_name}'
app.config['SQLALCHEMY_DATABASE_URI'] = db_config

db = SQLAlchemy()
engine = create_engine(db_config, pool_recycle= 3600, pool_pre_ping= True,  pool_size=10, max_overflow=5)
db.init_app(app)
api = Api(app,version='1.0', title='Truckx API',description='An aggregation and temperature creation API',)


ns = api.namespace('truckx', desctription = 'Aggregate and create API')

parser = reqparse.RequestParser()
parser.add_argument('start_timestamp', type=str, help='starting timestamp for aggregate data query in "YYYY-MM-DD HH:MM:SS" format')
parser.add_argument('end_timestamp', type=str, help='ending timestamp for aggregate data query in "YYYY-MM-DD HH:MM:SS" format')


class TemperatureData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sensor_id = db.Column(db.Integer, index = True,nullable=False)
    temperature = db.Column(db.Float, nullable=False)
    timestamp = db.Column(db.BigInteger, nullable=False)


class AggregateData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
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

    # @ns.expect(parser = parser)
    @ns.doc(parser = parser)
    def get(self, sensor_id):
        try:
            with engine.connect() as conn: 
                args = parser.parse_args()
                start_timestamp = args.get('start_timestamp')
                end_timestamp = args.get('end_timestamp')

                print(sensor_id, start_timestamp, end_timestamp, '[sensor_id, start_timestamp, end_timestamp]-[Aggregate]')

                query = text(
                    f"SELECT sensor_id, avg_temperature, max_temperature, min_temperature, timestamp FROM aggregate_data WHERE sensor_id = {sensor_id} "
                )

                if start_timestamp:
                    try:
                        ist_datetime = datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S')
                        utc_datetime = ist_datetime - timedelta(hours=5.5)
                        start_utc_timestamp = utc_datetime.timestamp()
                        query = text(
                            f"{query} AND timestamp >= {int(start_utc_timestamp)}"
                        )
                    except Exception as e:
                        print(e, '[e]')
                        return handle_error("Provide start timestamp in 'YYYY-MM-DD HH:MM:SS' format", 400)

                if end_timestamp:
                    try:
                        ist_datetime = datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S')
                        utc_datetime = ist_datetime - timedelta(hours=5.5)
                        end_utc_timestamp = utc_datetime.timestamp()
                        query = text(
                            f"{query} AND timestamp <= {int(end_utc_timestamp)}"
                        )
                    except Exception as e:
                        return handle_error("Provide end timestamp in 'YYYY-MM-DD HH:MM:SS' format", 400)
                
                query = text(f"{query} ORDER BY timestamp DESC LIMIT 1;")

                print(query, '[query]-[Aggregate class]')
                data = conn.execute(query).first()
                if not data:
                    return {'message': 'No aggregate data available for the specified sensor'}, 404

                data = data._asdict()
                print(data, '[data]-[Aggregate]')

                result = []

                timestamp = data['timestamp']
                utc_datetime = datetime.fromtimestamp(timestamp)
                ist_datetime = utc_datetime + timedelta(hours=5.5)

                result.append({
                    'sensor_id': data['sensor_id'],
                    'avg_temperature': data['avg_temperature'],
                    'max_temperature': data['max_temperature'],
                    'min_temperature': data['min_temperature'],
                    'timestamp': ist_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                })
                return result, 200

                '''query = AggregateData.query.filter_by(sensor_id=sensor_id)

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

                data = conn.execute(text(query.order_by(AggregateData.timestamp.desc()).limit(1)))

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
                return result, 200'''
        except Exception as e:
            print(str(e), '[error Aggregate]')
            return handle_error({'type': 'Internal Server Error','message': 'Something went wrong'}, 500)


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
            with engine.connect() as conn:
                data = request.get_json()
                print(data, '[data]-[add_temperature]')
                keys_to_check = ['sensor_id', 'temperature', 'timestamp']
                missing_keys = [key for key in keys_to_check if key not in data]

                print(missing_keys, '[missing_keys]-[Temperature class]')
                if missing_keys : 
                    return handle_error('All keys not present', 400)
                

                sensor_id = data['sensor_id']
                temperature = data['temperature']
                timestamp = data['timestamp']
                print(sensor_id, temperature, timestamp, '[sensor_id, temperature, timestamp]-[add_temperature]')

                try:
                    ist_datetime = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                    utc_datetime = ist_datetime - timedelta(hours=5.5)
                    utc_timestamp = utc_datetime.timestamp()
                except Exception as e:
                    return handle_error("Provide timestamp in 'YYYY-MM-DD HH:MM:SS' format", 400)

                query = f"INSERT INTO temperature_data (sensor_id, temperature, timestamp) VALUES ({sensor_id}, {temperature}, {utc_timestamp})"
                conn.execute(text(query))
                conn.commit()

                return {'message': f'Temperature data added successfully for sensor_id = {sensor_id}'}, 201 



                '''try:
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
                return {'message': f'Temperature data added successfully for sensor_id = {sensor_id}'}, 201'''
        except Exception as e:
            print(str(e), '[error in Temperature class]')
            return handle_error({'type': 'Internal Server Error','message': 'Something went wrong'}, 500)


def aggregate_data():

    try: 
        with app.app_context():
            with engine.connect() as conn: 
                sensors = conn.execute(text("SELECT DISTINCT sensor_id FROM temperature_data")).fetchall()
                now = datetime.utcnow()
                now_timestamp = int(now.timestamp())
                hour_ago = now - timedelta(hours=1)
                hour_ago_timestamp = int(hour_ago.timestamp())
                print(sensors, '[sensors]')

                result = []

                for sensor in sensors:
                    sensor_id = sensor[0]
                    aggregated_data = conn.execute(text(
                        f"SELECT AVG(temperature) as avg_temp, MAX(temperature) as max_temp, MIN(temperature) as min_temp "
                        f"FROM temperature_data "
                        f"WHERE sensor_id = :sensor_id AND timestamp >= :hour_ago_timestamp "
                        f"ORDER BY timestamp DESC LIMIT 1"
                    ), {'sensor_id': sensor_id, 'hour_ago_timestamp': hour_ago_timestamp}).first()._asdict()

                    print(aggregated_data, '[aggregated_data]')

                    if (aggregated_data is not None 
                        and aggregated_data['avg_temp'] is not None 
                        and aggregated_data['max_temp'] is not None 
                        and aggregated_data['min_temp'] is not None
                        ):

                        query = text(
                            "INSERT INTO aggregate_data (sensor_id, avg_temperature, max_temperature, min_temperature, timestamp) "
                            "VALUES (:sensor_id, :avg_temp, :max_temp, :min_temp, :now_timestamp)"
                        )

                        conex = conn.execute(query, {
                            'sensor_id': sensor_id,
                            'avg_temp': aggregated_data['avg_temp'],
                            'max_temp': aggregated_data['max_temp'],
                            'min_temp': aggregated_data['min_temp'],
                            'now_timestamp': now_timestamp
                        })
                        print(conex, '[conex]-[aggregate_data function]')
                        conn.commit()
                    else: 
                        result.append({'message' : f'No aggregation data for sensor = {sensor_id}'})
                return jsonify({'message': 'Aggregation completed successfully', 'details' : result}), 200

                '''sensors = db.session.query(TemperatureData.sensor_id).distinct().all()
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

                return jsonify({'message': 'Aggregation completed successfully'}), 200'''    
    except Exception as e: 
        # some mailing or message service could be employed here to make us aware of the exception
        print ( str(e), '[error ing aggregate_data function]')
        scheduler.shutdown()
        return jsonify({'message': str(e)}), 500



if __name__ == '__main__':
    
    # Scheduler for hourly aggregation
    scheduler = BackgroundScheduler()
    # Schedule the aggregation task every hour
    scheduler.add_job(func=aggregate_data,trigger= 'interval', hours = 1)
    scheduler.start()
    app.run(debug=True, host='0.0.0.0', port = 5000)
