import time
import csv
from memphis import Memphis
import asyncio
import glob
import os
import random
from datetime import datetime
from dotenv import load_dotenv


load_dotenv('../.env')
load_dotenv()

BENCHMARK_WARMUP_MINUTES = float(os.getenv('BENCHMARK_WARMUP_MINUTES'))
BENCHMARK_DURATION_MINUTES = float(os.getenv('BENCHMARK_DURATION_MINUTES'))
BENCHMARK_CONSUMER_AFTER_BENCHMARK_WAIT_MINUTES = float(os.getenv('BENCHMARK_CONSUMER_AFTER_BENCHMARK_WAIT_MINUTES'))

TOTAL_DURATION_SECONDS = (BENCHMARK_WARMUP_MINUTES + BENCHMARK_DURATION_MINUTES + BENCHMARK_CONSUMER_AFTER_BENCHMARK_WAIT_MINUTES) * 60.0

DELIVERY_MODE = os.getenv('DELIVERY_MODE')
MEMPHIS_CONSUMER_ACK_POLICY = os.getenv('MEMPHIS_CONSUMER_ACK_POLICY')

RESULT_BASEPATH = f'results/{os.getenv("PRODUCER_COUNT")}-{os.getenv("PARTITION_COUNT")}-{os.getenv("CONSUMER_COUNT")}-{os.getenv("DELIVERY_MODE")}/consumer'

MAX_POLL_RECORDS = int(os.getenv('CONSUMER_MAX_POLL_RECORDS'))

results = []


async def initialize():
    memphis = Memphis()
    await memphis.connect(
        host=os.getenv('MEMPHIS_BROKER_HOST'),
        username=os.getenv('MEMPHIS_USERNAME'),
        password=os.getenv('MEMPHIS_PASSWORD')
    )
    consumer = await memphis.consumer(
        station_name=os.getenv('MEMPHIS_TOPIC_NAME'),
        consumer_name=os.getenv('MEMPHIS_CONSUMER_NAME'),
        consumer_group=os.getenv('MEMPHIS_CONSUMER_GROUP_NAME')
    )
    return memphis,consumer


async def benchmark(consumer):
    global results

    # start_time = time.time()
    # warmup_seconds = BENCHMARK_WARMUP_MINUTES * 60.0
    # collection_start_time = start_time + warmup_seconds
    # collection_end_time = collection_start_time + (BENCHMARK_DURATION_MINUTES * 60.0)

    while True:
        t1 = time.time()
        messages = await consumer.fetch(batch_size=MAX_POLL_RECORDS)
        t2 = time.time()

        if not messages:
            await asyncio.sleep(random.random() * 0.000001)
            continue

        for message in messages:
            # if receive_time < collection_start_time:
            #     pass  # warmup
            # elif receive_time > collection_end_time:
            #     pass  # after benchmark
            # else:
            message_size = len(message.get_data())

            headers = message.get_headers()
            time_sent = float(headers['time_sent'])

            latency = t2 - time_sent
            processing_time = f'{(t2 - t1) * 1_000_000:.7f}'

            results.append([t2, message_size, processing_time, latency, -1])

            if MEMPHIS_CONSUMER_ACK_POLICY == 'explicit':
                await message.ack()

            # time.sleep(random.random() * 0.001)
            await asyncio.sleep(random.random() * 0.000001)


def cleanup_results():
    os.makedirs(RESULT_BASEPATH, exist_ok=True)
    
    csv_files = glob.glob(os.path.join(RESULT_BASEPATH, '*.csv'))
    for f in csv_files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error removing CSV file {f}: {e}")


def write_results_to_csv(results):
    filename = f'{os.getenv("CONSUMER_RESULT_CSV_FILENAME")}-{datetime.now().strftime("%m%d_%H%M%S")}-{os.getenv("CONSUMER_ID")}.csv'

    with open(os.path.join(RESULT_BASEPATH, filename), mode="w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["timestamp", "message_size", "processing_time", "latency", "lag"])
        csv_writer.writerows(results)


async def main():
    global results
    memphis, consumer = await initialize()
    cleanup_results()

    try:
        await asyncio.wait_for(benchmark(consumer), timeout=TOTAL_DURATION_SECONDS)
    except KeyboardInterrupt:
        pass
    except asyncio.TimeoutError:
        pass
    finally:
        await consumer.destroy()
        await memphis.close()

        print('writing results to csv...')
        write_results_to_csv(results)
        print('done.')


if __name__ == "__main__":
    asyncio.run(main())
