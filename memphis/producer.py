import time
import random
import string
import csv
from memphis import Memphis
import asyncio


BROKER_HOST = "localhost"
BENCHMARK_DURATION_SECONDS = 60 * 60

MESSAGE_MIN_SIZE = 10_000
MESSAGE_MAX_SIZE = 1_000_000

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
    len_dataset = 10_000
    for i in range(len_dataset):
        message = get_random_string()
        message_size = len(message)
        dataset.append({message.encode('utf-8'), message_size})
        if i % 500 == 0:
            print(f'generating dataset... ({(i/len_dataset)*100}%, {i} / {len_dataset})')
    
    print('successfully generated dataset.')
    return dataset


async def benchmark(producer, dataset):
    global results, total_bytes
    start_time = time.time()
    
    print(f'starting benchmark... start time={start_time}')
    counter = 0
    while time.time() - start_time < BENCHMARK_DURATION_SECONDS:
        data = dataset[counter]

        await producer.produce(data.message)

        total_bytes += data.message_size
        results.append([time.time(), data.message_size, total_bytes])

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

        print(f'successfully performed benchmark. end time={time.time()}')
        print('writing results to csv...')
        write_results_to_csv(results)
        print('done')


if __name__ == "__main__":
    asyncio.run(main())
