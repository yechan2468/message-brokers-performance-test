import os
from dotenv import load_dotenv
from visualize import *
from file_io import *
from util import *


load_dotenv()
load_dotenv('../.env')

VISUALIZATION_BROKERS = os.getenv('VISUALIZATION_BROKERS').split(',')
VISUALIZATION_RESULTS_DIR = os.getenv('VISUALIZATION_RESULTS_DIR')
BENCHMARK_WARMUP_MINUTES = float(os.getenv('BENCHMARK_WARMUP_MINUTES'))
BENCHMARK_DURATION_MINUTES = float(os.getenv('BENCHMARK_DURATION_MINUTES'))


def read_data(brokers, current_base_directory_name):
    producer_data = {}
    consumer_data = {}

    for broker in brokers:
        print(f'reading data from {broker}...', end=' ')
        start_time, end_time = read_benchmark_time(broker, current_base_directory_name)
        start_time += BENCHMARK_WARMUP_MINUTES * 60

        producer_data[broker] = read_producer_data(broker, current_base_directory_name, start_time, end_time)
        consumer_data[broker] = read_consumer_data(broker, current_base_directory_name, start_time, end_time)
        assert not producer_data[broker].empty, "Make sure benchmark duration is not too short"
        assert not consumer_data[broker].empty, "Make sure benchmark duration is not too short"
        print('done')
    
    return producer_data, consumer_data

def draw_graphs(brokers, base_directory_name, producer_data, consumer_data):
    print('drawing throughput graph... ', end=' ')
    draw_producer_throughput_graph(brokers, base_directory_name, producer_data)
    draw_consumer_throughput_graph(brokers, base_directory_name, consumer_data)
    print('done.')

    print('drawing latency graph... ', end=' ')
    draw_latency_boxplot(base_directory_name, consumer_data)
    draw_latency_histogram(base_directory_name, consumer_data)
    print('done.')

    print('drawing processing time graph... ', end=' ')
    draw_producer_processing_time_graph(brokers, base_directory_name, producer_data)
    draw_consumer_processing_time_graph(brokers, base_directory_name, consumer_data)
    print('done.')


def main(prod_count, part_count, cons_count, delivery_mode):
    base_directory_name = f'{prod_count}-{part_count}-{cons_count}-{delivery_mode}'

    if delivery_mode == 'EXACTLY_ONCE':
        if ('rabbitmq' in VISUALIZATION_BROKERS):
            VISUALIZATION_BROKERS.remove('rabbitmq')

    print('reading data...')
    producer_data, consumer_data = read_data(VISUALIZATION_BROKERS, base_directory_name)
    print('successfully read data.')

    initialize_directory(base_directory_name)

    print('drawing graphs...')
    draw_graphs(VISUALIZATION_BROKERS, base_directory_name, producer_data, consumer_data)
    print('successfully drew graphs.')

    print('visualization done')


if __name__ == '__main__':
    main()
