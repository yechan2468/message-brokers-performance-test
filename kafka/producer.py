import random
import string
import time
from datetime import datetime
import csv
import json
import os
from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv()

TOPIC = 'kafka'
BROKER = 'localhost:9092'
KAFKA_STAT_INTERVAL_MS = os.getenv('KAFKA_STAT_INTERVAL_MS')

BENCHMARK_WARMUP_MINUTES = int(os.getenv('BENCHMARK_WARMUP_MINUTES'))
BENCHMARK_DURATION_MINUTES = int(os.getenv('BENCHMARK_DURATION_MINUTES'))

DATASET_SIZE = int(os.getenv('DATASET_SIZE'))
MESSAGE_MIN_SIZE = int(os.getenv('MESSAGE_MIN_SIZE'))
MESSAGE_MAX_SIZE = int(os.getenv('MESSAGE_MAX_SIZE'))

RESULT_BENCHMARK_TIME_FILENAME = os.getenv('RESULT_BENCHMARK_TIME_FILENAME')
PRODUCER_RESULT_CSV_FILENAME = os.getenv('PRODUCER_RESULT_CSV_FILENAME')

producer_stats = {"tx_bytes": [0]}
start_time = 0.
end_time = 0.


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
        'statistics.interval.ms': KAFKA_STAT_INTERVAL_MS
    })
    
    return producer


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


def benchmark(producer, dataset, results):
    global start_time
    start_time = time.time()
    
    print(f'starting benchmark... start time={datetime.now()}')
    counter = 0
    benchmark_duration = (BENCHMARK_DURATION_MINUTES + BENCHMARK_WARMUP_MINUTES) * 60
    while (time.time() - start_time) < benchmark_duration:
        data = dataset[counter % DATASET_SIZE]
        try:
            producer.produce(TOPIC, data['message'], callback=report_delivery)
        except Exception as e:
            pass
        producer.poll(0)
            
        results.append([time.time(), data['message_size'], producer_stats["tx_bytes"][-1]])
        counter += 1
        
    producer.flush()

    return results


def write_benchmark_time():
    global start_time, end_time
    with open(RESULT_BENCHMARK_TIME_FILENAME, mode="w", newline="") as text_file:
        text_file.write(f'{start_time},{end_time}')


def write_results_to_csv(results):
    with open(PRODUCER_RESULT_CSV_FILENAME, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'message_size', 'byte_size'])
        csv_writer.writerows(results)


def main():
    global producer_stats, end_time

    producer = initialize()
    dataset = generate_dataset()
    
    results = []
    try:
        benchmark(producer, dataset, results)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        end_time = time.time()

        print(f'successfully performed benchmark. end time={datetime.now()}')
        print('writing results...')
        write_benchmark_time()
        write_results_to_csv(results)
        print('done.')


if __name__ == "__main__":
    main()
