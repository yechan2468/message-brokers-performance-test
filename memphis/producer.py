import time
from datetime import datetime
import random
import string
import csv
import os
import asyncio
from memphis import Memphis, Headers
from dotenv import load_dotenv


load_dotenv()

BROKER_HOST = "localhost"
STATION_NAME = "memphis-station"
PRODUCER_NAME = "memphis-producer"
USERNAME = 'test'
PASSWORD = 'Test123456!@'

BENCHMARK_WARMUP_MINUTES = int(os.getenv('BENCHMARK_WARMUP_MINUTES'))
BENCHMARK_DURATION_MINUTES = int(os.getenv('BENCHMARK_DURATION_MINUTES'))

DATASET_SIZE = int(os.getenv('DATASET_SIZE'))
MESSAGE_MIN_SIZE = int(os.getenv('MESSAGE_MIN_SIZE'))
MESSAGE_MAX_SIZE = int(os.getenv('MESSAGE_MAX_SIZE'))

RESULT_BENCHMARK_TIME_FILENAME = os.getenv('RESULT_BENCHMARK_TIME_FILENAME')
PRODUCER_RESULT_CSV_FILENAME = os.getenv('PRODUCER_RESULT_CSV_FILENAME')


results = []
total_bytes = 0
start_time = 0.
end_time = 0.


async def initialize():
    memphis = Memphis()
    await memphis.connect(host=BROKER_HOST, username=USERNAME, password=PASSWORD)
    producer = await memphis.producer(station_name=STATION_NAME, producer_name=PRODUCER_NAME)
    return memphis, producer


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
    global results, total_bytes, start_time
    start_time = time.time()
    
    print(f'starting benchmark... start time={datetime.now()}')
    counter = 0
    benchmark_duration = (BENCHMARK_DURATION_MINUTES + BENCHMARK_WARMUP_MINUTES) * 60
    while (time.time() - start_time) < benchmark_duration:
        data = dataset[counter % DATASET_SIZE]

        headers = Headers()
        headers.add('time_sent', str(time.time()))

        await producer.produce(
            message=bytearray(data['message']),
            headers=headers
        )

        total_bytes += data['message_size']
        results.append([time.time(), data['message_size'], total_bytes])

        counter += 1


def write_benchmark_time():
    global start_time, end_time
    with open(RESULT_BENCHMARK_TIME_FILENAME, mode="w", newline="") as text_file:
        text_file.write(f'{start_time},{end_time}')


def write_results_to_csv(results):
    with open(PRODUCER_RESULT_CSV_FILENAME, mode="w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["timestamp", "message_size", "byte_size"])
        csv_writer.writerows(results)


async def main():
    global results, end_time
    memphis, producer = await initialize()
    dataset = generate_dataset()

    try:
        await benchmark(producer, dataset)
    except KeyboardInterrupt:
        pass
    finally:
        await memphis.close()
        end_time = time.time()

        print(f'successfully performed benchmark. end time={datetime.now()}')
        print('writing results...')
        write_benchmark_time()
        write_results_to_csv(results)
        print('done')


if __name__ == "__main__":
    asyncio.run(main())
