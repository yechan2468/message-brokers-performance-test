import pika
import random
import string
import time
import csv

QUEUE = 'rabbitmq'
BROKER = 'localhost'
BENCHMARK_DURATION_SECONDS = 60

MESSAGE_MIN_SIZE = 100
MESSAGE_MAX_SIZE = 1000

RESULT_CSV_FILENAME = 'producer_metrics.csv'


def get_random_string():
    length = random.randint(MESSAGE_MIN_SIZE, MESSAGE_MAX_SIZE)
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def initialize():
    connection = pika.BlockingConnection(pika.ConnectionParameters(BROKER))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE)
    return connection,channel


def benchmark(channel, results):
    message = get_random_string()
    message_size = len(message)
    byte_size = len(message.encode('utf-8'))

    properties = pika.BasicProperties(timestamp=int(time.time()))
    channel.basic_publish(exchange='', routing_key=QUEUE, body=message, properties=properties)
        
    results.append([time.time(), message_size, byte_size])


def write_results_to_csv(results):
    with open(RESULT_CSV_FILENAME, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'message_size', 'byte_size'])
        csv_writer.writerows(results)


def main():
    connection, channel = initialize()
    results = []
    start_time = time.time()
    
    while time.time() - start_time < BENCHMARK_DURATION_SECONDS:
        benchmark(channel, results)

    connection.close()

    print('writing results to csv...')
    write_results_to_csv(results)
    print('done.')


if __name__ == "__main__":
    main()
