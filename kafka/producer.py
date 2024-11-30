import random
import string
import time
import csv
from confluent_kafka import Producer
import json

TOPIC = 'kafka'
BROKER = 'localhost:9092'
BENCHMARK_DURATION_SECONDS = 60

MESSAGE_MIN_SIZE = 100
MESSAGE_MAX_SIZE = 1000

STAT_INTERVAL_MS = 10 * 1000

RESULT_CSV_FILENAME = 'producer_metrics.csv'

producer_stats = {"tx_bytes": [0]}


def stats_callback(stats_json_str):
    global producer_stats

    stats = json.loads(stats_json_str)
    producer_stats["tx_bytes"].append(stats["tx_bytes"])


def get_random_string():
    length = random.randint(MESSAGE_MIN_SIZE, MESSAGE_MAX_SIZE)
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def report_delivery(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')


def initialize():
    producer = Producer({
        'bootstrap.servers': BROKER,
        'stats_cb': stats_callback,
        'statistics.interval.ms': STAT_INTERVAL_MS
    })
    
    return producer


def benchmark(producer, start_time, results):
    while time.time() - start_time < BENCHMARK_DURATION_SECONDS:
        message = get_random_string().encode('utf-8')
        message_size = len(message.decode('utf-8'))
            
        producer.produce(TOPIC, message, callback=report_delivery)
        producer.poll(0)
            
        results.append([time.time(), message_size, producer_stats["tx_bytes"][-1]])
        time.sleep(0.1)
        
    producer.flush()

    return results


def write_results_to_csv(results):
    with open(RESULT_CSV_FILENAME, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'message_size', 'byte_size'])
        csv_writer.writerows(results)


def main():
    global producer_stats

    producer = initialize()
    start_time = time.time()
    results = []
    
    try:
        benchmark(producer, start_time, results)
    except KeyboardInterrupt:
        pass
    finally:
        print('writing results to csv...')
        write_results_to_csv(results)
        print('done.')


if __name__ == "__main__":
    main()
