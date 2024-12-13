import random
import string
import time
from datetime import datetime
import csv
import json
import os
import sys
from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv()

TOPIC = 'redpanda'
BROKER = 'localhost:19092'
KAFKA_STAT_INTERVAL_MS = os.getenv('KAFKA_STAT_INTERVAL_MS')

BENCHMARK_WARMUP_MINUTES = int(os.getenv('BENCHMARK_WARMUP_MINUTES'))
BENCHMARK_DURATION_MINUTES = int(os.getenv('BENCHMARK_DURATION_MINUTES'))

DATASET_SIZE = int(os.getenv('DATASET_SIZE'))

RESULT_BENCHMARK_TIME_FILENAME = os.getenv('RESULT_BENCHMARK_TIME_FILENAME')
PRODUCER_RESULT_CSV_FILENAME = os.getenv('PRODUCER_RESULT_CSV_FILENAME')

PRODUCER_ID = 0
MESSAGE_SIZE_KIB = 0
VALID_PRODUCER_IDS = list(map(int, os.getenv('VALID_PRODUCER_IDS').split(',')))
VALID_MESSAGE_SIZES_KIB = list(map(float, os.getenv('VALID_MESSAGE_SIZES_KIB').split(',')))


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


def get_producer_parameters():
    global PRODUCER_ID, MESSAGE_SIZE_KIB

    if len(sys.argv) < 3:
        print('python producer.py <producer id> <message size in KiB>')
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
            'message': message.encode('utf-8'),
            'message_size': MESSAGE_SIZE_KIB * 1024
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
    with open(f'{RESULT_BENCHMARK_TIME_FILENAME}-{PRODUCER_ID}.txt', mode="w", newline="") as text_file:
        text_file.write(f'{start_time},{end_time}')


def write_results_to_csv(results):
    with open(f'{PRODUCER_RESULT_CSV_FILENAME}-{PRODUCER_ID}.csv', mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'message_size', 'byte_size'])
        csv_writer.writerows(results)


def main():
    global producer_stats, end_time

    get_producer_parameters()
    producer = initialize()
    dataset = generate_dataset()
    
    results = []
    try:
        benchmark(producer, dataset, results)
    except KeyboardInterrupt:
        pass
    finally:
        end_time = time.time()

        print(f'successfully performed benchmark. end time={datetime.now()}')
        print('writing results...')
        write_benchmark_time()
        write_results_to_csv(results)
        print('done.')


if __name__ == "__main__":
    main()
