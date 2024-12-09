import time
from datetime import datetime
import random
import string
import csv
import os
import asyncio
from memphis import Memphis
from dotenv import load_dotenv


load_dotenv()

BROKER_HOST = "localhost"
BENCHMARK_DURATION_MINUTES = int(os.getenv('BENCHMARK_DURATION_MINUTES'))

DATASET_SIZE = int(os.getenv('DATASET_SIZE'))

MESSAGE_MIN_SIZE = int(os.getenv('MESSAGE_MIN_SIZE'))
MESSAGE_MAX_SIZE = int(os.getenv('MESSAGE_MAX_SIZE'))

STATION_NAME = "memphis-station"
PRODUCER_NAME = "memphis-producer"
USERNAME = 'test'
PASSWORD = 'Test123456!@'

RESULT_CSV_FILENAME = 'producer_metrics.csv'


results = []
total_bytes = 0


async def initialize():
    memphis = Memphis()
    await memphis.connect(host=BROKER_HOST, username=USERNAME, password=PASSWORD)
    producer = await memphis.producer(station_name=STATION_NAME, producer_name=PRODUCER_NAME)
    return memphis,producer



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


async def benchmark(producer, dataset):
    global results, total_bytes
    start_time = time.time()
    
    print(f'starting benchmark... start time={datetime.now()}')
    counter = 0
    while time.time() - start_time < BENCHMARK_DURATION_MINUTES * 60:
        data = dataset[counter % DATASET_SIZE]

        await producer.produce(data['message'])

        total_bytes += data['message_size']
        results.append([time.time(), data['message_size'], total_bytes])

        counter += 1


def write_results_to_csv(results):
    with open(RESULT_CSV_FILENAME, mode="w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["timestamp", "message_size", "byte_size"])
        csv_writer.writerows(results)


async def main():
    global results
    memphis, producer = await initialize()
    dataset = generate_dataset()

    try:
        await benchmark(producer, dataset)
    except KeyboardInterrupt:
        pass
    finally:
        await memphis.close()

        print(f'successfully performed benchmark. end time={datetime.now()}')
        print('writing results to csv...')
        write_results_to_csv(results)
        print('done')


if __name__ == "__main__":
    asyncio.run(main())
