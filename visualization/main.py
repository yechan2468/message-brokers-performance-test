import os
import sys
from dotenv import load_dotenv
from visualize import *
from file_io import *


load_dotenv()
load_dotenv('../.env')

VISUALIZATION_BROKERS = os.getenv('VISUALIZATION_BROKERS').split(',')
VISUALIZATION_RESULTS_DIR = os.getenv('VISUALIZATION_RESULTS_DIR')
BENCHMARK_WARMUP_MINUTES = float(os.getenv('BENCHMARK_WARMUP_MINUTES'))


def initialize_directory(base_directory_name):
    base_dir = os.path.join(VISUALIZATION_RESULTS_DIR, base_directory_name)
    os.makedirs(base_dir, exist_ok=True)

    for filename in os.listdir(base_dir):
        item_path = os.path.join(base_dir, filename)
        if os.path.isfile(item_path):
            os.remove(item_path)


def read_data(brokers, base_directory_name):
    producer_data = {}
    consumer_data = {}
    resource_usage_data = {}

    for broker in brokers:
        print(f'reading data from {broker}...', end=' ')
        start_time, end_time = read_benchmark_time(broker, base_directory_name)
        start_time += BENCHMARK_WARMUP_MINUTES * 60

        producer_data[broker] = read_producer_data(broker, base_directory_name, start_time, end_time)
        consumer_data[broker] = read_consumer_data(broker, base_directory_name, start_time, end_time)
        assert not producer_data[broker].empty 
        assert not consumer_data[broker].empty

        resource_usage_data[broker] = {}
        # resource_usage_data[broker]['cpu'] = read_cpu_usage_data(broker)
        # resource_usage_data[broker]['memory'] = read_memory_usage_data(broker)
        print('done.')

    return producer_data, consumer_data, resource_usage_data


def draw_graphs(brokers, base_directory_name, producer_data, consumer_data, resource_usage_data):
    draw_producer_throughput_graph(brokers, base_directory_name, producer_data)
    draw_consumer_throughput_graph(brokers, base_directory_name, consumer_data)

    draw_latency_boxplot(base_directory_name, consumer_data)
    draw_latency_histogram(base_directory_name, consumer_data)

    draw_producer_processing_time_graph(brokers, base_directory_name, producer_data)
    draw_consumer_processing_time_graph(brokers, base_directory_name, consumer_data)

    # draw_lag_graph(consumer_data)

    # draw_cpu_usage_graph(resource_usage_data)
    # draw_memory_usage_graph(resource_usage_data)


def main():
    prod_count, part_count, cons_count, delivery_mode = sys.argv[1:]
    match delivery_mode:
        case 'L': delivery_mode = 'AT_LEAST_ONCE'
        case 'M': delivery_mode = 'AT_MOST_ONCE'
        case 'E': delivery_mode = 'EXACTLY_ONCE'
        case 'AT_LEAST_ONCE': delivery_mode = 'AT_LEAST_ONCE'
        case 'AT_MOST_ONCE': delivery_mode = 'AT_MOST_ONCE'
        case 'EXACTLY_ONCE': delivery_mode = 'EXACTLY_ONCE'
        case _: 
            print(f'invalid delivery mode input: {delivery_mode}')
            return
    base_directory_name = f'{prod_count}-{part_count}-{cons_count}-{delivery_mode}'

    if delivery_mode == 'EXACTLY_ONCE':
        if ('rabbitmq' in VISUALIZATION_BROKERS):
            VISUALIZATION_BROKERS.remove('rabbitmq')

    print('reading data...')
    producer_data, consumer_data, resource_usage_data = read_data(VISUALIZATION_BROKERS, base_directory_name)
    print('successfully read data.')

    initialize_directory(base_directory_name)

    print('drawing graphs...')
    draw_graphs(VISUALIZATION_BROKERS, base_directory_name, producer_data, consumer_data, resource_usage_data)
    print('successfully drew graphs.')

    print('visualization done')


if __name__ == '__main__':
    main()
