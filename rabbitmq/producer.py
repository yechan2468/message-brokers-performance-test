import pika
import random
import string
import time
import csv


QUEUE = 'rabbitmq'
BROKER = 'localhost'
BENCHMARK_DURATION_SECONDS = 60 * 60

MESSAGE_MIN_SIZE = 10_000
MESSAGE_MAX_SIZE = 1_000_000

RESULT_CSV_FILENAME = 'producer_metrics.csv'


def initialize():
    connection = pika.BlockingConnection(pika.ConnectionParameters(BROKER))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE)
    return connection,channel




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


def benchmark(channel, dataset, results):
    start_time = time.time()

    print(f'starting benchmark... start time={start_time}')
    counter = 0
    while time.time() - start_time < BENCHMARK_DURATION_SECONDS:
        data = dataset[counter]
        byte_size = len(data.message)

        properties = pika.BasicProperties(timestamp=int(time.time()))
        channel.basic_publish(exchange='', routing_key=QUEUE, body=data.message, properties=properties)
            
        results.append([time.time(), data.message_size, byte_size])
        counter += 1


def write_results_to_csv(results):
    with open(RESULT_CSV_FILENAME, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'message_size', 'byte_size'])
        csv_writer.writerows(results)


def main():
    connection, channel = initialize()
    results = []
    dataset = generate_dataset()

    try:
        benchmark(channel, dataset, results)
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()

        print(f'successfully performed benchmark. end time={time.time()}')
        print('writing results to csv...')
        write_results_to_csv(results)
        print('done.')


if __name__ == "__main__":
    main()
