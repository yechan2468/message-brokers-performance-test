import time
from datetime import datetime
import random
import string
import csv
import os
import asyncio
import sys
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

RESULT_BENCHMARK_TIME_FILENAME = os.getenv('RESULT_BENCHMARK_TIME_FILENAME')
PRODUCER_RESULT_CSV_FILENAME = os.getenv('PRODUCER_RESULT_CSV_FILENAME')

PRODUCER_ID = 0
MESSAGE_SIZE_KIB = 0
VALID_PRODUCER_IDS = list(map(int, os.getenv('VALID_PRODUCER_IDS').split(',')))
VALID_MESSAGE_SIZES_KIB = list(map(float, os.getenv('VALID_MESSAGE_SIZES_KIB').split(',')))

results = []
start_time = 0.
end_time = 0.


async def initialize():
    memphis = Memphis()
    await memphis.connect(host=BROKER_HOST, username=USERNAME, password=PASSWORD)
    producer = await memphis.producer(station_name=STATION_NAME, producer_name=PRODUCER_NAME)
    return memphis, producer


def get_producer_parameters():
    global PRODUCER_ID, MESSAGE_SIZE_KIB

    if len(sys.argv) < 3:
        print('python producer.py <number of producers> <message size in KiB>')
        exit()

    PRODUCER_ID = int(sys.argv[1])
    if PRODUCER_ID not in VALID_PRODUCER_IDS:
        print(f'Number of producers should be one of {VALID_PRODUCER_IDS}, but received {PRODUCER_ID}')
        exit()

    MESSAGE_SIZE_KIB = float(sys.argv[2])
    if MESSAGE_SIZE_KIB not in VALID_MESSAGE_SIZES_KIB:
        print(f'Message size should be one of {VALID_MESSAGE_SIZES_KIB}, but received {MESSAGE_SIZE_KIB}')
        exit()


def generate_random_bytes(size_in_bytes):
    return os.urandom(int(size_in_bytes))


def generate_dataset():
    dataset = []
    for i in range(DATASET_SIZE):
        message = generate_random_bytes(MESSAGE_SIZE_KIB * 1024)
        dataset.append({
            'message': message, 
            'message_size': MESSAGE_SIZE_KIB * 1024
        })
        if i % 500 == 0:
            print(f'generating dataset... ({(i/DATASET_SIZE)*100}%, {i} / {DATASET_SIZE})')
    
    print('successfully generated dataset.')
    return dataset


async def benchmark(producer, dataset):
    global results, start_time
    start_time = time.time()
    
    print(f'starting benchmark... start time={datetime.now()}')
    counter = 0
    benchmark_duration = (BENCHMARK_DURATION_MINUTES + BENCHMARK_WARMUP_MINUTES) * 60
    while (time.time() - start_time) < benchmark_duration:
        data = dataset[counter % DATASET_SIZE]

        headers = Headers()
        headers.add('time_sent', str(time.time()))

        t1 = time.time()
        await producer.produce(
            message=bytearray(data['message']),
            headers=headers
        )
        t2 = time.time()
        processing_time = f'{(t2 - t1) * 1_000_000:.7f}'

        results.append([t2, data['message_size'], processing_time])

        counter += 1
        time.sleep(random.random() * 0.001)


def write_benchmark_time():
    global start_time, end_time
    with open(f'{RESULT_BENCHMARK_TIME_FILENAME}-{PRODUCER_ID}.txt', mode="w", newline="") as text_file:
        text_file.write(f'{start_time},{end_time}')


def write_results_to_csv(results):
    with open(f'{PRODUCER_RESULT_CSV_FILENAME}-{PRODUCER_ID}.csv', mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["timestamp", "message_size", "processing_time"])
        csv_writer.writerows(results)


async def main():
    global results, end_time

    get_producer_parameters()
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
