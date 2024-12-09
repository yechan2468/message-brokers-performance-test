import os
from dotenv import load_dotenv
from visualize import *
from file_io import *


load_dotenv()

BROKERS = os.getenv('BROKERS').split(',')
RESULTS_DIR = os.getenv('RESULTS_DIR')


def initialize_directory():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    for file in os.listdir(RESULTS_DIR):
        os.remove(os.path.join(RESULTS_DIR, file))


def read_data():
    producer_data = {}
    consumer_data = {}
    resource_usage_data = {}

    for broker in BROKERS:
        print(f'reading data from {broker}...', end=' ')
        producer_data[broker] = read_producer_data(broker)
        consumer_data[broker] = read_consumer_data(broker)

        resource_usage_data[broker] = {}
        resource_usage_data[broker]['cpu'] = read_cpu_usage_data(broker)
        resource_usage_data[broker]['memory'] = read_memory_usage_data(broker)
        print('done.')

    return producer_data, consumer_data, resource_usage_data


def draw_graphs(producer_data, consumer_data, resource_usage_data):
    draw_broker_throughput_byterate_boxplots(producer_data, consumer_data)
    draw_producer_throughput_byterate_boxplots(producer_data)
    draw_consumer_throughput_byterate_boxplots(consumer_data)

    draw_latency_boxplot(consumer_data)
    draw_latency_histogram(consumer_data)

    draw_lag_graph(consumer_data)

    # draw_cpu_usage_graph(resource_usage_data)
    # draw_memory_usage_graph(resource_usage_data)


def main():
    print('reading data...')
    producer_data, consumer_data, resource_usage_data = read_data()
    print('successfully read data.')

    initialize_directory()

    print('drawing graphs...')
    draw_graphs(producer_data, consumer_data, resource_usage_data)
    print('successfully drew graphs.')

    print('done.')


if __name__ == '__main__':
    main()
