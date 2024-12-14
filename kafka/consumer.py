import time
import json
import csv
import os
from confluent_kafka import Consumer
from dotenv import load_dotenv


load_dotenv()

TOPIC = 'kafka'
BROKER = 'localhost:9092'
GROUP_ID = 'consumer-group'

KAFKA_STAT_INTERVAL_MS = int(os.getenv('KAFKA_STAT_INTERVAL_MS'))

RESULT_CSV_FILENAME = 'consumer_metrics'


def stats_callback(stats_json_str):
    pass


def initialize():
    consumer = Consumer({
        'bootstrap.servers': BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'stats_cb': stats_callback,
        'statistics.interval.ms': KAFKA_STAT_INTERVAL_MS
    })
    consumer.subscribe([TOPIC])
    return consumer


def benchmark(consumer, results):
    while True:
        t1 = time.time()
        message = consumer.poll(timeout=60.0)
        t2 = time.time()

        if message is None:
            continue
        if message.error():
            print(f"Consumer error: {message.error()}")
            break
            
        payload_size = len(message)
        latency = t2 - message.timestamp()[1] / 1000.0  # in seconds
        processing_time = f'{(t2 - t1) * 1_000_000:.7f}'

        results.append([t2, payload_size, processing_time, latency, -1])
    
    return results


def write_results_to_csv(results):
    with open(f'{RESULT_CSV_FILENAME}.csv', mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'message_size', 'processing_time', 'latency', 'lag'])
        csv_writer.writerows(results)


def main():
    consumer = initialize()
    
    results = []
    try:
        benchmark(consumer, results)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print('writing results to csv...')
        write_results_to_csv(results)
        print('done.')   
        

if __name__ == "__main__":
    main()
