import time
import csv
import os
import glob
from datetime import datetime
import random
import json
from confluent_kafka import Consumer
from dotenv import load_dotenv


load_dotenv('../.env')
load_dotenv()


DELIVERY_MODE = os.getenv('DELIVERY_MODE')
RESULT_BASEPATH = f'results/{os.getenv("PRODUCER_COUNT")}-{os.getenv("PARTITION_COUNT")}-{os.getenv("CONSUMER_COUNT")}-{os.getenv("DELIVERY_MODE")}/consumer'
MAX_POLL_RECORDS = int(os.getenv('CONSUMER_MAX_POLL_RECORDS'))


def stats_callback(stats_json_str):
    pass


def initialize():
    ISOLATION_LEVEL = os.getenv('KAFKA_CONSUMER_ISOLATION_LEVEL')
    ENABLE_AUTO_COMMIT = DELIVERY_MODE not in ['AT_LEAST_ONCE', 'EXACTLY_ONCE']

    consumer = Consumer({
        'bootstrap.servers': f'{os.getenv("KAFKA_BROKER_HOSTNAME")}:{os.getenv("KAFKA_BROKER_PORT")}',
        'group.id': os.getenv('KAFKA_CONSUMER_GROUP_ID'),
        'auto.offset.reset': 'earliest',
        'stats_cb': stats_callback,
        'statistics.interval.ms': int(os.getenv('KAFKA_STAT_INTERVAL_MS')),

        # delivery
        'enable.auto.commit': ENABLE_AUTO_COMMIT,
        'isolation.level': ISOLATION_LEVEL
    })

    consumer.subscribe([os.getenv('KAFKA_TOPIC_NAME')])
    return consumer


def benchmark(consumer, results):    
    start_time = time.time()
    total_duration_seconds = (float(os.getenv('BENCHMARK_WARMUP_MINUTES')) \
                              + float(os.getenv('BENCHMARK_DURATION_MINUTES')) \
                              + float(os.getenv('BENCHMARK_CONSUMER_AFTER_BENCHMARK_WAIT_MINUTES'))) * 60.0
    end_time_limit = start_time + total_duration_seconds

    while time.time() < end_time_limit:
        t1 = time.time()
        messages = consumer.consume(num_messages=MAX_POLL_RECORDS, timeout=1.0)
        t2 = time.time()

        if not messages:
            continue

        last_processed_message = None 
        
        for message in messages:

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {message.error()}")
                return results
        
            try:
                message_value = json.loads(message.value().decode('utf-8'))
                time_sent = float(message_value['time_sent_ms']) / 1000.0  # sec
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Warning: Failed to extract producer timestamp. Error: {e}. Using broker timestamp.")
                time_sent = message.timestamp()[1] / 1000.0
                
            payload_size = len(message)
            latency = t2 - time_sent  # in seconds
            processing_time = f'{(t2 - t1) * 1_000_000:.7f}'

            results.append([t2, payload_size, processing_time, latency, -1])

            last_processed_message = message

            if DELIVERY_MODE in ['AT_LEAST_ONCE', 'EXACTLY_ONCE']:
                try:
                    consumer.commit(message=last_processed_message, asynchronous=True)
                except Exception as commit_err:
                    print(f"Commit failed: {commit_err}")

            time.sleep(random.random() * 0.000001)
    
    return results


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

    with open(os.path.join(RESULT_BASEPATH, filename), mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'message_size', 'processing_time', 'latency', 'lag'])
        csv_writer.writerows(results)


def main():
    print(f'init time={datetime.now()}')

    cleanup_results()

    consumer = initialize()
    
    results = []
    try:
        print(f"starting benchmark... start time={datetime.now()} consumer id={os.getenv('CONSUMER_ID')}")
        benchmark(consumer, results)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print('writing results to csv...')
        write_results_to_csv(results)
        print(f'done. end time={datetime.now()}')   
        

if __name__ == "__main__":
    main()
