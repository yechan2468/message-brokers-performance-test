import os
from dotenv import load_dotenv
from visualize import *
from file_io import *


load_dotenv()

VISUALIZATION_BROKERS = os.getenv('VISUALIZATION_BROKERS').split(',')
VISUALIZATION_RESULTS_DIR = os.getenv('VISUALIZATION_RESULTS_DIR')
BENCHMARK_WARMUP_MINUTES = int(os.getenv('BENCHMARK_WARMUP_MINUTES'))


def initialize_directory():
    os.makedirs(VISUALIZATION_RESULTS_DIR, exist_ok=True)
    for file in os.listdir(VISUALIZATION_RESULTS_DIR):
        os.remove(os.path.join(VISUALIZATION_RESULTS_DIR, file))


def read_data():
    producer_data = {}
    consumer_data = {}
    resource_usage_data = {}

    for broker in VISUALIZATION_BROKERS:
        print(f'reading data from {broker}...', end=' ')
        start_time, end_time = read_benchmark_time(broker)
        start_time += BENCHMARK_WARMUP_MINUTES * 60

        producer_data[broker] = read_producer_data(broker, start_time, end_time)
        consumer_data[broker] = read_consumer_data(broker, start_time, end_time)

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

    # draw_lag_graph(consumer_data)

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
