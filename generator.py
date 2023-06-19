from faker import Faker
from google.cloud import pubsub_v1
import argparse
import random
import json
import datetime
import time



def publish(publisher, topic, message):
    data = message.encode('utf-8')
    return publisher.publish(topic_path, data=data)


def generate_tweep():
    data = {}
    data['created_at'] = datetime.datetime.now().strftime('%d/%b/%Y:%H:%M:%S')
    data['tweep_id'] = faker.uuid4()
    data['text'] = faker.sentence()
    data['user'] = random.choice(usernames)
    return json.dumps(data)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--project-id', type=str, required=True)
    parser.add_argument('--topic', type=str, required=True)
    args = parser.parse_args()

    print(args.project_id)

    usernames = []
    faker = Faker()
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project_id, args.topic)

    for i in range(200):
        newprofile = faker.simple_profile()
        usernames.append(newprofile['username'])
    print("Hit CTRL-C to stop Tweeping!")

    counter = 0
    while True:
        publish(publisher, topic_path, generate_tweep())
        time.sleep(0.5)
        counter += 1
        print(counter)
