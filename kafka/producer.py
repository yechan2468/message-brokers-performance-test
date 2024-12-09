import random
import string
import time
import csv
from confluent_kafka import Producer
import json


TOPIC = 'kafka'
BROKER = 'localhost:9092'
BENCHMARK_DURATION_SECONDS = 60 * 60

MESSAGE_MIN_SIZE = 10_000
MESSAGE_MAX_SIZE = 1_000_000

STAT_INTERVAL_MS = 10 * 1000

RESULT_CSV_FILENAME = 'producer_metrics.csv'

producer_stats = {"tx_bytes": [0]}


def stats_callback(stats_json_str):
    global producer_stats

    stats = json.loads(stats_json_str)
    producer_stats["tx_bytes"].append(stats["tx_bytes"])


def report_delivery(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}\n{msg}')


def initialize():
    producer = Producer({
        'bootstrap.servers': BROKER,
        'stats_cb': stats_callback,
        'statistics.interval.ms': STAT_INTERVAL_MS
    })
    
    return producer


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


def benchmark(producer, dataset, results):
    start_time = time.time()
    
    print(f'starting benchmark... start time={start_time}')
    counter = 0
    while time.time() - start_time < BENCHMARK_DURATION_SECONDS:
        data = dataset[counter]
        try:
            producer.produce(TOPIC, data.message, callback=report_delivery)
        except Exception as e:
            print(e)
        producer.poll(0)
            
        results.append([time.time(), data.message_size, producer_stats["tx_bytes"][-1]])
        counter += 1
        
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
    dataset = generate_dataset()
    
    results = []
    try:
        benchmark(producer, dataset, results)
    except KeyboardInterrupt:
        pass
    finally:
        print(f'successfully performed benchmark. end time={time.time()}')
        print('writing results to csv...')
        write_results_to_csv(results)
        print('done.')


if __name__ == "__main__":
    main()
