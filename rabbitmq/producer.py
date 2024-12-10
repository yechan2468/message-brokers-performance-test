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

BENCHMARK_WARMUP_MINUTES = int(os.getenv('BENCHMARK_WARMUP_MINUTES'))
BENCHMARK_DURATION_MINUTES = int(os.getenv('BENCHMARK_DURATION_MINUTES'))

DATASET_SIZE = int(os.getenv('DATASET_SIZE'))
MESSAGE_MIN_SIZE = int(os.getenv('MESSAGE_MIN_SIZE'))
MESSAGE_MAX_SIZE = int(os.getenv('MESSAGE_MAX_SIZE'))

RESULT_BENCHMARK_TIME_FILENAME = os.getenv('RESULT_BENCHMARK_TIME_FILENAME')
PRODUCER_RESULT_CSV_FILENAME = os.getenv('PRODUCER_RESULT_CSV_FILENAME')


start_time = 0.
end_time = 0.


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
    global start_time
    start_time = time.time()

    print(f'starting benchmark... start time={datetime.now()}')
    counter = 0
    benchmark_duration = (BENCHMARK_DURATION_MINUTES + BENCHMARK_WARMUP_MINUTES) * 60
    while (time.time() - start_time) < benchmark_duration:
        data = dataset[counter % DATASET_SIZE]
        byte_size = len(data['message'])

        properties = pika.BasicProperties(timestamp=int(time.time()))
        channel.basic_publish(exchange='', routing_key=QUEUE, body=data['message'], properties=properties)
            
        results.append([time.time(), data['message_size'], byte_size])
        counter += 1


def write_benchmark_time():
    global start_time, end_time
    with open(RESULT_BENCHMARK_TIME_FILENAME, mode="w", newline="") as text_file:
        text_file.write(f'{start_time},{end_time}')


def write_results_to_csv(results):
    with open(PRODUCER_RESULT_CSV_FILENAME, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'message_size', 'byte_size'])
        csv_writer.writerows(results)


def main():
    global end_time

    connection, channel = initialize()
    results = []
    dataset = generate_dataset()

    try:
        benchmark(channel, dataset, results)
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()
        end_time = time.time()

        print(f'successfully performed benchmark. end time={datetime.now()}')
        print('writing results...')
        write_benchmark_time()
        write_results_to_csv(results)
        print('done.')


if __name__ == "__main__":
    main()
