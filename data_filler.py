from sqlalchemy import create_engine
from  sqlalchemy.orm import sessionmaker
from temp import TemperatureData
from datetime import datetime as dt
import time
import random

engine = create_engine('mysql://roshan:roshan@localhost:3306/truckx')
Session = sessionmaker(engine)

session = Session()
print('session created')

def insert_temperature_data () :
    sensor_ids = [1,2,3,4,5,6,7,8,9,10]
    n = 7200
    now = dt.now()

    for id in sensor_ids:
        timestamp_now = int(now.timestamp())
        i = 0
        temperature_objects_list = []
        start_time = time.time()
        while(i<=n):
            temperature_objects_list.append(
                TemperatureData(
                    sensor_id = id,
                    temperature = round(random.uniform(23,50),2),
                    timestamp = timestamp_now - i
                )
            )
            # breakpoint
            # print(f"temperature object created for i = {i} for sensor id = {id}")
            i += 1
            if(i %100 == 0):
                # print("*********************starting of add_all()*************")
                session.add_all(temperature_objects_list)
                session.commit()

                # print('************* SUCESSFULLY ADDED ALL OBJECTS ********************************')
                # breakpoint

        print('Time taken to insert for one sensor id = ', id, ' and time = ', time.time() - start_time)

if __name__ == '__main__':
    insert_temperature_data()