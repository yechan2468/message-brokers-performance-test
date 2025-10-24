import time
import csv
import glob
from datetime import datetime
import os
import hashlib
import pika
import threading
from dotenv import load_dotenv


load_dotenv('../.env')
load_dotenv()


QUEUE = 'rabbitmq'

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_BROKER_PORT = int(os.getenv('RABBITMQ_BROKER_PORT'))
RABBITMQ_QUEUE_PREFIX = os.getenv('RABBITMQ_QUEUE_PREFIX')
PARTITION_COUNT = int(os.getenv('PARTITION_COUNT', 3))

CONSUMER_ID = os.getenv('CONSUMER_ID')
RESULT_CSV_FILENAME = os.getenv('CONSUMER_RESULT_CSV_FILENAME')

BENCHMARK_WARMUP_MINUTES = float(os.getenv('BENCHMARK_WARMUP_MINUTES'))
BENCHMARK_DURATION_MINUTES = float(os.getenv('BENCHMARK_DURATION_MINUTES'))
BENCHMARK_CONSUMER_AFTER_BENCHMARK_WAIT_MINUTES = float(os.getenv('BENCHMARK_CONSUMER_AFTER_BENCHMARK_WAIT_MINUTES'))

DELIVERY_MODE = os.getenv('DELIVERY_MODE')
RABBITMQ_AUTO_ACK = os.getenv('RABBITMQ_AUTO_ACK', 'true').lower()


class ConsumerBenchmark:
    def __init__(self, consumer_id):
        self.results = []
        self.consumer_id = consumer_id
        
        self.queue_index = get_deterministic_index(self.consumer_id, PARTITION_COUNT)
        self.target_queue = f'{RABBITMQ_QUEUE_PREFIX}-{self.queue_index}'
        
        self.start_time = time.time()

        self.warmup_seconds = BENCHMARK_WARMUP_MINUTES * 60.0
        self.benchmark_seconds = BENCHMARK_DURATION_MINUTES * 60.0
        self.post_benchmark_wait = BENCHMARK_CONSUMER_AFTER_BENCHMARK_WAIT_MINUTES * 60.0

        self.collection_start_time = self.start_time + self.warmup_seconds
        self.collection_end_time = self.collection_start_time + self.benchmark_seconds
        self.total_duration_seconds = self.warmup_seconds + self.benchmark_seconds + self.post_benchmark_wait

    def consume_message(self, ch, method, properties, body):
        receive_time = time.time()
        
        if receive_time < self.collection_start_time: # warm up
            if not RABBITMQ_AUTO_ACK:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        elif receive_time > self.collection_end_time: # after benchmark
            if not RABBITMQ_AUTO_ACK:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        else:
            payload_size = len(body)
            latency = receive_time - (properties.timestamp / 1000.0)
            # queue_state = ch.queue_declare(queue=QUEUE, passive=True)
            # lag = queue_state.method.message_count
            lag = -1
            
            self.results.append([receive_time, payload_size, (time.time() - receive_time) * 1_000_000, latency, lag])

            if not RABBITMQ_AUTO_ACK:
                ch.basic_ack(delivery_tag=method.delivery_tag)

def initialize(benchmark_instance):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_BROKER_PORT
        ))
        channel = connection.channel()
    except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}")
        raise

    try:
        channel.queue_declare(queue=benchmark_instance.target_queue, durable=True, passive=False)
    except pika.exceptions.ChannelClosedByBroker:
        print(f"Error: Target queue {benchmark_instance.target_queue} declaration failed.")
        connection.close()
        raise
    
    print(f"Consumer ID {benchmark_instance.consumer_id} subscribing to queue: {benchmark_instance.target_queue}")
    
    channel.basic_qos(prefetch_count=1)
    
    channel.basic_consume(
        queue=benchmark_instance.target_queue, 
        on_message_callback=benchmark_instance.consume_message, 
        auto_ack=RABBITMQ_AUTO_ACK
    )
    return connection, channel


def benchmark(channel):
    channel.start_consuming()


def get_deterministic_index(unique_id_str, partition_count):
    if not unique_id_str:
        return 0
    
    hash_object = hashlib.sha256(unique_id_str.encode('utf-8'))
    hash_int = int(hash_object.hexdigest(), 16)
    return hash_int % partition_count


def shutdown_consumer(connection, total_duration_seconds):
    time.sleep(total_duration_seconds)
    
    if connection and connection.is_open:
        try:
            connection.add_callback_threadsafe(connection.ioloop.stop)
        except Exception as e:
            pass
    
    if connection and connection.is_open:
        try:
            connection.close() 
        except pika.exceptions.ConnectionWrongStateError:
            pass 
        except Exception as e:
            pass


def cleanup_results():
    result_dir = os.path.dirname(RESULT_CSV_FILENAME)
    
    csv_files = glob.glob(os.path.join(result_dir, '*.csv'))
    for f in csv_files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error removing CSV file {f}: {e}")


def write_results_to_csv(results, consumer_id):
    os.makedirs('results', exist_ok=True)
    filename = f'{RESULT_CSV_FILENAME}-{consumer_id}.csv'
    with open(filename, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'message_size', 'processing_time', 'latency', 'lag'])
        csv_writer.writerows(results)


def main():
    consumer_id = os.getenv('CONSUMER_ID')
    
    cleanup_results()
    
    benchmark_instance = ConsumerBenchmark(consumer_id)
    
    connection, channel = initialize(benchmark_instance)

    timer_thread = threading.Thread(
        target=shutdown_consumer, 
        args=(connection, benchmark_instance.total_duration_seconds)
    )
    timer_thread.daemon = True
    timer_thread.start()

    try:
        print(f"starting benchmark... start time={datetime.now()} consumer id={CONSUMER_ID}")
        benchmark(channel)
    except KeyboardInterrupt:
        print("Interrupted by user.")
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Consumer loop terminated by signal: {e}")
    finally:
        if connection and connection.is_open:
            try:
                connection.close() 
            except pika.exceptions.ConnectionWrongStateError:
                pass

        print('writing results to csv...')
        write_results_to_csv(benchmark_instance.results, consumer_id)
        print(f'done. end time={datetime.now()}')


if __name__ == "__main__":
    main()
