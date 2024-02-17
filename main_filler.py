from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from main2 import TemperatureData, db, app
from datetime import datetime as dt
import time
import random

with app.app_context():
    db.create_all()
    print(db, '[db.mainfiller]')

def insert_temperature_data():
    sensor_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    n = 7200
    now = dt.utcnow()
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
                    # print(f'added for i = {i} and session_id = {id}')
                    # print(temperature_objects_list)
                    db.session.add_all(temperature_objects_list)
                    db.session.commit()

            print('Time taken to insert for one sensor id = ', id, ' and time = ', time.time() - start_time)

if __name__ == '__main__':
    insert_temperature_data()
