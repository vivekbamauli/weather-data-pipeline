import time
import json
import requests
from kafka import KafkaProducer

API_KEY = "open weather api key"
# city = "delhi"
KAFKA_TOPIC = "bamauli"
KAFKA_BROKER = "localhost:9092"  


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')   
)
cities = ["Delhi", "Mumbai", "Kolkata", "Chennai", "Bangalore", "Hyderabad"]

def get_data(city):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"
    response = requests.get(url)
    return response.json()


while True:
    for city in cities:
        data = get_data(city)
        data["city"] = city
        print(f"sending data of {city},{data}")
        producer.send(KAFKA_TOPIC,data)
        producer.flush()
        time.sleep(10)

