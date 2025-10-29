import random
import time
from datetime import datetime
import csv
import os
import glob
import json
from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv


load_dotenv('../.env')
load_dotenv()


PRODUCER_ID = os.getenv('PRODUCER_ID')
DELIVERY_MODE = os.getenv('DELIVERY_MODE')

RESULT_BASEPATH = f'results/{os.getenv("PRODUCER_COUNT")}-{os.getenv("PARTITION_COUNT")}-{os.getenv("CONSUMER_COUNT")}-{os.getenv("DELIVERY_MODE")}/producer'

start_time = 0.
end_time = 0.


def stats_callback(stats_json_str):
    pass


def report_delivery(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}\n{msg}')


def initialize():
    producer_config = {
        'bootstrap.servers': f'{os.getenv("KAFKA_BROKER_HOSTNAME")}:{os.getenv("KAFKA_BROKER_PORT")}',
        'stats_cb': stats_callback,
        'statistics.interval.ms': os.getenv('KAFKA_STAT_INTERVAL_MS'),

        # delivery
        'acks': int(os.getenv('KAFKA_ACKS')),
        'retries': int(os.getenv('KAFKA_RETRIES')),
        'enable.idempotence': os.getenv('KAFKA_ENABLE_IDEMPOTENCE', 'false').lower() == 'true'
    }

    transactional_id = os.getenv('KAFKA_PRODUCER_TRANSACTIONAL_ID', None)
    if DELIVERY_MODE == 'EXACTLY_ONCE' and transactional_id:
        producer_config['transactional.id'] = transactional_id
    
    producer = Producer(producer_config)

    if DELIVERY_MODE == 'EXACTLY_ONCE':
        producer.init_transactions()

    return producer


def generate_dataset():
    dataset = []
    DATASET_SIZE = int(os.getenv('DATASET_SIZE'))
    MESSAGE_SIZE_KIB = int(os.getenv('MESSAGE_SIZE_KIB'))
    metadata_size = 50
    total_message_size = MESSAGE_SIZE_KIB * 1024
    random_data_size = max(total_message_size - metadata_size, 0)

    for i in range(DATASET_SIZE):
        payload_bytes = os.urandom(random_data_size)

        message = {
            'time_sent_ms': 0,
            'payload': payload_bytes.hex()
        }

        dataset.append({
            'message': message,
            'message_size': total_message_size
        })
        if i % 500 == 0:
            print(f'generating dataset... ({(i/DATASET_SIZE)*100}%, {i} / {DATASET_SIZE})')
    
    print('successfully generated dataset.')
    return dataset


def benchmark(producer, dataset, results):
    global start_time
    start_time = time.time()
    
    counter = 0
    benchmark_duration = (float(os.getenv('BENCHMARK_DURATION_MINUTES')) + float(os.getenv('BENCHMARK_WARMUP_MINUTES'))) * 60
    topic_name = os.getenv('KAFKA_TOPIC_NAME')

    while (time.time() - start_time) < benchmark_duration:
        data = dataset[counter % int(os.getenv('DATASET_SIZE'))]
        
        message_to_send_dict = data['message'].copy()
        message_to_send_dict['time_sent_ms'] = int(time.time() * 1000)
        final_message_bytes = json.dumps(message_to_send_dict).encode('utf-8')
        
        t1 = time.time()
        try:
            if DELIVERY_MODE == 'EXACTLY_ONCE':
                producer.begin_transaction()
            
            producer.produce(topic_name, final_message_bytes, callback=report_delivery)
            producer.poll(0)

            if DELIVERY_MODE == 'EXACTLY_ONCE':
                producer.commit_transaction()

            
            t2 = time.time()

            processing_time = f'{(t2 - t1) * 1_000_000:.7f}'
            results.append([t2, data['message_size'], processing_time])
            
        except KafkaException as e:
            if DELIVERY_MODE == 'EXACTLY_ONCE':
                producer.abort_transaction()
            print(f'Kafka Producer Error: {e}')
        except Exception as e:
            print(f'Producer General Error: {e}')
    
        counter += 1
        time.sleep(random.random() * 0.001)
        
    producer.flush()

    return results


def cleanup_results():
    os.makedirs(RESULT_BASEPATH, exist_ok=True)
    
    csv_files = glob.glob(os.path.join(RESULT_BASEPATH, '*.csv'))
    for f in csv_files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error removing CSV file {f}: {e}")
            
    txt_files = glob.glob(os.path.join(RESULT_BASEPATH, '*.txt'))
    for f in txt_files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error removing TXT file {f}: {e}")


def write_benchmark_time():
    global start_time, end_time
    filename = f'{os.getenv("RESULT_BENCHMARK_TIME_FILENAME")}-{datetime.now().strftime("%m%d_%H%M%S")}-{PRODUCER_ID}.txt'

    with open(os.path.join(RESULT_BASEPATH, filename), mode="w", newline="") as text_file:
        text_file.write(f'{start_time},{end_time}')


def write_results_to_csv(results):
    filename = f'{os.getenv("PRODUCER_RESULT_CSV_FILENAME")}-{datetime.now().strftime("%m%d_%H%M%S")}-{PRODUCER_ID}.csv'

    with open(os.path.join(RESULT_BASEPATH, filename), mode='w', newline='') as csv_file:
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
