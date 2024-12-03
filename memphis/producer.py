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


def get_random_string():
    length = random.randint(MESSAGE_MIN_SIZE, MESSAGE_MAX_SIZE)
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


async def initialize():
    memphis = Memphis()
    await memphis.connect(host=BROKER_HOST, username=USERNAME, password=PASSWORD)
    producer = await memphis.producer(station_name=STATION_NAME, producer_name=PRODUCER_NAME)
    return memphis,producer


async def benchmark(producer):
    global results, total_bytes
    start_time = time.time()
    while time.time() - start_time < BENCHMARK_DURATION_SECONDS:
        message = get_random_string()
        message_size = len(message.encode('utf-8'))

        await producer.produce(message)

        total_bytes += message_size
        results.append([time.time(), message_size, total_bytes])


def write_results_to_csv(results):
    with open(RESULT_CSV_FILENAME, mode="w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["timestamp", "message_size", "byte_size"])
        csv_writer.writerows(results)


async def main():
    global results
    memphis, producer = await initialize()

    try:
        await benchmark(producer)
    except KeyboardInterrupt:
        pass
    finally:
        await memphis.close()

        print('writing results to csv...')
        write_results_to_csv(results)
        print('done')


if __name__ == "__main__":
    asyncio.run(main())
