import pika
import time
import csv
import threading

QUEUE = 'rabbitmq'
BROKER = 'localhost'

RESULT_CSV_FILENAME = 'consumer_metrics.csv'

results = []
rx_bytes = -1


def consume_message(ch, method, properties, body):
    global results, rx_bytes

    receive_time = time.time()
    payload_size = len(body.decode('utf-8'))
    latency = receive_time - properties.timestamp
    
    queue_state = ch.queue_declare(queue=QUEUE, passive=True)
    lag = queue_state.method.message_count
    
    results.append([receive_time, payload_size, rx_bytes, latency, lag])


def initialize():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=BROKER,
        frame_max=128*1024
    ))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE)
    
    channel.basic_consume(queue=QUEUE, on_message_callback=consume_message, auto_ack=True)
    return connection,channel


def benchmark(channel):
    channel.start_consuming()


def write_results_to_csv():
    with open(RESULT_CSV_FILENAME, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'message_size', 'byte_size', 'latency', 'lag'])
        csv_writer.writerows(results)


def main():
    global results, rx_bytes
    connection, channel = initialize()
    
    try:
        benchmark(channel)
    except KeyboardInterrupt:
        pass
    finally:
        channel.stop_consuming()
        connection.close()

        print('writing results to csv...')
        write_results_to_csv()
        print('done.')



if __name__ == "__main__":
    main()
