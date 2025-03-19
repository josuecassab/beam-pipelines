from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError
from json import dumps
from random import randint
import time
import json


with open("config.json") as file:
    config = json.load(file)

producer = KafkaProducer(
    bootstrap_servers=[config['bootstrap.servers']],
    security_protocol=config['security.protocol'],
    sasl_mechanism=config['sasl.mechanism'],
    sasl_plain_username=config['sasl.username'],
    sasl_plain_password=config['sasl.password'],
)

fake = Faker()

records = []

for i in range(5):
    data = {
        'id': fake.unique.random_int(min=10000, max=99999),
        'name': fake.name(),
        'email': fake.email(),
        'phone': fake.phone_number(),
        'address': fake.address(),
        'city': fake.city(),
        'timestamp': time.time(),
    }
    for i in range(randint(1, 4)):

        record = {**data,
                  'quantity': randint(1, 10),
                  'price': randint(100, 200),
                  }
        
        print(record)
        json_data = dumps(record).encode('utf-8')
        future = producer.send('source', json_data)

        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as e:
            # Decide what to do if produce request failed...
            print(e)
            
        time.sleep(5)