import pika
import random
import string
import time
from datetime import datetime
import csv
import os
from dotenv import load_dotenv


load_dotenv()


QUEUE = 'rabbitmq'
BROKER = 'localhost'
BENCHMARK_DURATION_MINUTES = int(os.getenv('BENCHMARK_DURATION_MINUTES'))

DATASET_SIZE = int(os.getenv('DATASET_SIZE'))

MESSAGE_MIN_SIZE = int(os.getenv('MESSAGE_MIN_SIZE'))
MESSAGE_MAX_SIZE = int(os.getenv('MESSAGE_MAX_SIZE'))

RESULT_CSV_FILENAME = 'producer_metrics.csv'


def initialize():
    connection = pika.BlockingConnection(pika.ConnectionParameters(BROKER))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE)
    return connection,channel


def get_random_string():
    length = random.randint(MESSAGE_MIN_SIZE, MESSAGE_MAX_SIZE)
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_dataset():
    dataset = []
    for i in range(DATASET_SIZE):
        message = get_random_string()
        message_size = len(message)
        dataset.append({
            'message': message.encode('utf-8'),
            'message_size': message_size
        })
        if i % 500 == 0:
            print(f'generating dataset... ({(i/DATASET_SIZE)*100}%, {i} / {DATASET_SIZE})')
    
    print('successfully generated dataset.')
    return dataset


def benchmark(channel, dataset, results):
    start_time = time.time()

    print(f'starting benchmark... start time={datetime.now()}')
    counter = 0
    while time.time() - start_time < BENCHMARK_DURATION_MINUTES * 60:
        data = dataset[counter % DATASET_SIZE]
        byte_size = len(data['message'])

        properties = pika.BasicProperties(timestamp=int(time.time()))
        channel.basic_publish(exchange='', routing_key=QUEUE, body=data['message'], properties=properties)
            
        results.append([time.time(), data['message_size'], byte_size])
        counter += 1


def write_results_to_csv(results):
    with open(RESULT_CSV_FILENAME, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'message_size', 'byte_size'])
        csv_writer.writerows(results)


def main():
    connection, channel = initialize()
    results = []
    dataset = generate_dataset()

    try:
        benchmark(channel, dataset, results)
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()

        print(f'successfully performed benchmark. end time={datetime.now()}')
        print('writing results to csv...')
        write_results_to_csv(results)
        print('done.')


if __name__ == "__main__":
    main()
