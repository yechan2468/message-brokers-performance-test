import random
import time
from datetime import datetime
import csv
import os
import glob
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv('../.env')
load_dotenv()


BENCHMARK_WARMUP_MINUTES = float(os.getenv('BENCHMARK_WARMUP_MINUTES'))
BENCHMARK_DURATION_MINUTES = float(os.getenv('BENCHMARK_DURATION_MINUTES'))

DATASET_SIZE = int(os.getenv('DATASET_SIZE'))

RESULT_BENCHMARK_TIME_FILENAME = os.getenv('RESULT_BENCHMARK_TIME_FILENAME')
PRODUCER_RESULT_CSV_FILENAME = os.getenv('PRODUCER_RESULT_CSV_FILENAME')

PRODUCER_ID = os.getenv('PRODUCER_ID')

start_time = 0.
end_time = 0.


def stats_callback(stats_json_str):
    pass


def report_delivery(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}\n{msg}')


def initialize():
    producer = Producer({
        'bootstrap.servers': f'{os.getenv("KAFKA_BROKER_HOSTNAME")}:{os.getenv("KAFKA_BROKER_PORT")}',
        'stats_cb': stats_callback,
        'statistics.interval.ms': os.getenv('KAFKA_STAT_INTERVAL_MS')
    })
    
    return producer


def generate_random_bytes(size_in_bytes):
    return os.urandom(int(size_in_bytes))


def generate_dataset():
    dataset = []
    for i in range(DATASET_SIZE):
        MESSAGE_SIZE_KIB = int(os.getenv('MESSAGE_SIZE_KIB'))
        message = generate_random_bytes(MESSAGE_SIZE_KIB * 1024)
        dataset.append({
            'message': message,
            'message_size': MESSAGE_SIZE_KIB * 1024
        })
        if i % 500 == 0:
            print(f'generating dataset... ({(i/DATASET_SIZE)*100}%, {i} / {DATASET_SIZE})')
    
    print('successfully generated dataset.')
    return dataset


def benchmark(producer, dataset, results):
    global start_time
    start_time = time.time()
    
    counter = 0
    benchmark_duration = (BENCHMARK_DURATION_MINUTES + BENCHMARK_WARMUP_MINUTES) * 60
    topic_name = os.getenv('KAFKA_TOPIC_NAME')

    while (time.time() - start_time) < benchmark_duration:
        data = dataset[counter % DATASET_SIZE]
        
        t1 = time.time()
        try:
            producer.produce(topic_name, data['message'], callback=report_delivery)
        except Exception as e:
            pass
        producer.poll(0)
        t2 = time.time()

        processing_time = f'{(t2 - t1) * 1_000_000:.7f}'
        results.append([t2, data['message_size'], processing_time])
        counter += 1
        time.sleep(random.random() * 0.001)
        
    producer.flush()

    return results


def cleanup_results():
    result_dir = os.path.dirname(PRODUCER_RESULT_CSV_FILENAME)
    
    csv_files = glob.glob(os.path.join(result_dir, '*.csv'))
    for f in csv_files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error removing CSV file {f}: {e}")
            
    txt_files = glob.glob(os.path.join(result_dir, '*.txt'))
    for f in txt_files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error removing TXT file {f}: {e}")


def write_benchmark_time():
    global start_time, end_time
    with open(f'{RESULT_BENCHMARK_TIME_FILENAME}-{PRODUCER_ID}.txt', mode="w", newline="") as text_file:
        text_file.write(f'{start_time},{end_time}')


def write_results_to_csv(results):
    with open(f'{PRODUCER_RESULT_CSV_FILENAME}-{PRODUCER_ID}.csv', mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'message_size', 'processing_time'])
        csv_writer.writerows(results)


def main():
    global end_time
    print(f'init time={datetime.now()}')

    cleanup_results()
    producer = initialize()
    dataset = generate_dataset()
    
    results = []
    try:
        print(f'starting benchmark... start time={datetime.now()} producer id: {os.getenv("PRODUCER_ID")} ')
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
        print(f'done. end time={datetime.now()}')


if __name__ == "__main__":
    main()
