import sys
import json
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass


sys.path.append(str(Path(__file__).parent.parent))

@dataclass
class Ride:
    lpep_pickup_datetime: int
    lpep_dropoff_datetime: int
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float

def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)

from kafka import KafkaConsumer

server = 'localhost:9092'
topic_name = 'green_trips'  
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    value_deserializer=ride_deserializer,
    auto_offset_reset='earliest',
    group_id='green_trips_console',
)

print(f"Listening to {topic_name}...")

count = 0
for message in consumer:
    ride = message.value
    trip_distance = ride.trip_distance
    if trip_distance > 5:
        count += 1
        print(f"Count: {count} ")

consumer.close()
print(f"Received {count} messages")