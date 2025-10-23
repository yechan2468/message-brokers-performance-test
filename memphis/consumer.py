import time
import csv
from memphis import Memphis
import asyncio
import glob
import os
from dotenv import load_dotenv


load_dotenv('../.env')
load_dotenv()

BENCHMARK_WARMUP_MINUTES = float(os.getenv('BENCHMARK_WARMUP_MINUTES'))
BENCHMARK_DURATION_MINUTES = float(os.getenv('BENCHMARK_DURATION_MINUTES'))
BENCHMARK_CONSUMER_AFTER_BENCHMARK_WAIT_MINUTES = float(os.getenv('BENCHMARK_CONSUMER_AFTER_BENCHMARK_WAIT_MINUTES'))

TOTAL_DURATION_SECONDS = (BENCHMARK_WARMUP_MINUTES + BENCHMARK_DURATION_MINUTES + BENCHMARK_CONSUMER_AFTER_BENCHMARK_WAIT_MINUTES) * 60.0


results = []


async def initialize():
    memphis = Memphis()
    await memphis.connect(
        host=os.getenv('MEMPHIS_BROKER_HOST'),
        username=os.getenv('MEMPHIS_USERNAME'),
        password=os.getenv('MEMPHIS_PASSWORD')
    )
    consumer = await memphis.consumer(
        station_name=os.getenv('MEMPHIS_STATION_NAME'),
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
        messages = await consumer.fetch()
        t2 = time.time()

        for message in messages:
            # if t2 < collection_start_time:
            #     pass  # warmup
            # elif t2 > collection_end_time:
            #     pass  # after benchmark
            # else:
            message_size = len(message.get_data())

            headers = message.get_headers()
            time_sent = float(headers['time_sent'])

            latency = t2 - time_sent
            processing_time = f'{(t2 - t1) * 1_000_000:.7f}'

            results.append([t2, message_size, processing_time, latency, -1])

            await message.ack()
            # time.sleep(random.random() * 0.001)
            # await asyncio.sleep(random.random() * 0.001)


def cleanup_results():
    result_dir = os.path.dirname(os.getenv('CONSUMER_RESULT_CSV_FILENAME'))
    
    csv_files = glob.glob(os.path.join(result_dir, '*.csv'))
    for f in csv_files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error removing CSV file {f}: {e}")


def write_results_to_csv(results):
    with open(f'{os.getenv("CONSUMER_RESULT_CSV_FILENAME")}-{os.getenv("CONSUMER_ID")}.csv', mode="w", newline="") as csv_file:
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
