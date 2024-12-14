import time
import csv
from memphis import Memphis
import asyncio
import sys


BROKER_HOST = "localhost"

STATION_NAME = "memphis-station"
CONSUMER_NAME = 'memphis-consumer'
CONSUMER_GROUP_NAME = 'memphis-consumer-group'
USERNAME = 'test2'
PASSWORD = 'Test123456@!'

RESULT_CSV_FILENAME = "consumer_metrics"


results = []


async def initialize():
    memphis = Memphis()
    await memphis.connect(host=BROKER_HOST, username=USERNAME, password=PASSWORD)
    consumer = await memphis.consumer(station_name=STATION_NAME, consumer_name=CONSUMER_NAME, consumer_group=CONSUMER_GROUP_NAME)
    return memphis,consumer


async def benchmark(consumer):
    global results

    while True:
        t1 = time.time()
        messages = await consumer.fetch()
        t2 = time.time()

        for message in messages:
            message_size = len(message.get_data())

            headers = message.get_headers()
            time_sent = float(headers['time_sent'])

            latency = t2 - time_sent
            processing_time = f'{(t2 - t1) * 1_000_000:.7f}'

            results.append([t2, message_size, processing_time, latency, -1])

            await message.ack()


def write_results_to_csv(results):
    with open(f'{RESULT_CSV_FILENAME}.csv', mode="w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["timestamp", "message_size", "latency", "total_bytes"])
        csv_writer.writerows(results)


async def main():
    global results
    memphis, consumer = await initialize()

    try:
        await benchmark(consumer)
    except KeyboardInterrupt:
        pass
    finally:
        await memphis.close()

        print('writing results to csv...')
        write_results_to_csv(results)
        print('done.')


if __name__ == "__main__":
    asyncio.run(main())
