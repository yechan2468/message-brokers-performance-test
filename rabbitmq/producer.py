import pika
import random
import time
from datetime import datetime
import csv
import os
import glob
from dotenv import load_dotenv
from pika.exceptions import AMQPChannelError, AMQPConnectionError


load_dotenv('../.env')
load_dotenv()


DELIVERY_MODE = os.getenv('DELIVERY_MODE') 
RABBITMQ_CONFIRM_MODE = os.getenv('RABBITMQ_CONFIRM_MODE').lower()

QUEUE_NAME = 'rabbitmq'

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_BROKER_PORT = int(os.getenv('RABBITMQ_BROKER_PORT'))
RABBITMQ_QUEUE_PREFIX = os.getenv('RABBITMQ_QUEUE_PREFIX')
PARTITION_COUNT = int(os.getenv('PARTITION_COUNT')) 

BENCHMARK_WARMUP_MINUTES = float(os.getenv('BENCHMARK_WARMUP_MINUTES'))
BENCHMARK_DURATION_MINUTES = float(os.getenv('BENCHMARK_DURATION_MINUTES'))

DATASET_SIZE = int(os.getenv('DATASET_SIZE'))

RESULT_BENCHMARK_TIME_FILENAME = os.getenv('RESULT_BENCHMARK_TIME_FILENAME')
PRODUCER_RESULT_CSV_FILENAME = os.getenv('PRODUCER_RESULT_CSV_FILENAME')
PRODUCER_ID = os.getenv('PRODUCER_ID')

MESSAGE_SIZE_KIB = int(os.getenv('MESSAGE_SIZE_KIB'))

RABBITMQ_EXCHANGE_NAME = f'{RABBITMQ_QUEUE_PREFIX}-exchange'

RESULT_BASEPATH = f'results/{os.getenv("PRODUCER_COUNT")}-{os.getenv("PARTITION_COUNT")}-{os.getenv("CONSUMER_COUNT")}-{os.getenv("DELIVERY_MODE")}/producer'

start_time = 0.
end_time = 0.


def initialize():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_BROKER_PORT
    ))
    channel = connection.channel()
    # channel.queue_declare(queue=QUEUE_NAME)

    if RABBITMQ_CONFIRM_MODE:
        # channel.confirm_delivery()
        channel.tx_select()

    return connection,channel


def generate_random_bytes(size_in_bytes):
    return os.urandom(int(size_in_bytes))


def generate_dataset():
    dataset = []
    for i in range(DATASET_SIZE):
        message = generate_random_bytes(MESSAGE_SIZE_KIB * 1024)
        dataset.append({
            'message': message,
            'message_size': MESSAGE_SIZE_KIB * 1024
        })
        if i % 500 == 0:
            print(f'generating dataset... ({(i/DATASET_SIZE)*100}%, {i} / {DATASET_SIZE})')
    
    print('successfully generated dataset.')
    return dataset


def benchmark(channel, dataset, results):
    global start_time
    start_time = time.time()

    counter = 0
    benchmark_duration = (BENCHMARK_DURATION_MINUTES + BENCHMARK_WARMUP_MINUTES) * 60

    while (time.time() - start_time) < benchmark_duration:
        data = dataset[counter % DATASET_SIZE]       

        queue_index = counter % PARTITION_COUNT
        routing_key = f'{RABBITMQ_QUEUE_PREFIX}-{queue_index}'

        t1 = time.time()
        try:
            properties = pika.BasicProperties(
                timestamp=int(time.time() * 1000), # ms
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
            channel.basic_publish(exchange=RABBITMQ_EXCHANGE_NAME, routing_key=routing_key, body=data['message'], properties=properties)
        
            if RABBITMQ_CONFIRM_MODE:
                # if not channel.wait_for_confirms(timeout=10):
                #     print(f"Warning: [{DELIVERY_MODE}] Broker NACK or Confirmation Timeout. Message may be resent or lost.")
                #     continue
                channel.tx_commit()

        except (AMQPChannelError, AMQPConnectionError) as e:
            if RABBITMQ_CONFIRM_MODE:
                channel.tx_rollback()
                print(f"RabbitMQ Transaction Error: Rollback executed. Message failed to deliver. {e}")
            else:
                print(f"RabbitMQ Channel Error during publish ({DELIVERY_MODE}): {e}")
        except Exception as e:
            print(f"Producer General Error during publish: {e}")
            continue
        t2 = time.time()

        processing_time = f'{(t2 - t1) * 1_000_000:.7f}'

        results.append([t2, data['message_size'], processing_time])
        counter += 1
        time.sleep(random.random() * 0.000001)


def cleanup_results():
    os.makedirs(RESULT_BASEPATH, exist_ok=True)
    
    for ext in ['*.csv', '*.txt']:
        for f in glob.glob(os.path.join(RESULT_BASEPATH, ext)):
            try:
                os.remove(f)
            except OSError as e:
                print(f"Error removing file {f}: {e}")


def write_benchmark_time():
    global start_time, end_time
    filename = f'{RESULT_BENCHMARK_TIME_FILENAME}-{datetime.now().strftime("%m%d_%H%M%S")}-{PRODUCER_ID}.txt'

    with open(os.path.join(RESULT_BASEPATH, filename), mode="w", newline="") as text_file:
        text_file.write(f'{start_time},{end_time}')


def write_results_to_csv(results):
    filename = f'{PRODUCER_RESULT_CSV_FILENAME}-{datetime.now().strftime("%m%d_%H%M%S")}-{PRODUCER_ID}.csv'

    with open(os.path.join(RESULT_BASEPATH, filename), mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'message_size', 'processing_time'])
        csv_writer.writerows(results)


def main():
    global end_time
    print(f'init time={datetime.now()}')

    cleanup_results()

    connection, channel = initialize()
    results = []
    dataset = generate_dataset()

    try:
        print(f'starting benchmark... start time={datetime.now()} producer id: {PRODUCER_ID} ')
        benchmark(channel, dataset, results)
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()
        end_time = time.time()

        print(f'successfully performed benchmark. end time={datetime.now()}')
        print('writing results...')
        write_benchmark_time()
        write_results_to_csv(results)
        print('done.')


if __name__ == "__main__":
    main()
