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
BENCHMARK_DURATION_MINUTES = float(os.getenv('BENCHMARK_DURATION_MINUTES'))


def initialize_directory(base_directory_name):
    base_dir = os.path.join(VISUALIZATION_RESULTS_DIR, base_directory_name)
    os.makedirs(base_dir, exist_ok=True)

    for filename in os.listdir(base_dir):
        item_path = os.path.join(base_dir, filename)
        if os.path.isfile(item_path):
            os.remove(item_path)


def read_data(brokers, current_base_directory_name, all_base_directory_names):
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
        
    all_resource_data = {}
    
    print(f'reading resource data...', end=' ')
    for base_directory_name in all_base_directory_names:
        resource_data_for_dir = {}        
        for broker in brokers:
            if ('rabbitmq' in brokers and base_directory_name.endswith('EXACTLY_ONCE')) and broker == 'rabbitmq':
                add_dummy_resource_data(resource_data_for_dir, broker) 
                continue

            start_time, end_time = read_benchmark_time(broker, base_directory_name) 
            start_time += BENCHMARK_WARMUP_MINUTES * 60

            resource_data_for_dir[broker] = {}
            resource_data_for_dir[broker]['cpu'] = read_cpu_usage_data(start_time, end_time)
            resource_data_for_dir[broker]['memory'] = read_memory_usage_data(start_time, end_time)
            resource_data_for_dir[broker]['iops'] = read_disk_iops_data(start_time, end_time)
            resource_data_for_dir[broker]['iowait'] = read_iowait_data(start_time, end_time)
            resource_data_for_dir[broker]['throughput'] = read_disk_throughput_data(start_time, end_time)
            assert not resource_data_for_dir[broker]['cpu'].empty 
            assert not resource_data_for_dir[broker]['memory'].empty
            assert not resource_data_for_dir[broker]['iops'].empty
            assert not resource_data_for_dir[broker]['iowait'].empty
            assert not resource_data_for_dir[broker]['throughput'].empty

        all_resource_data[base_directory_name] = resource_data_for_dir
    print('done.')

    resource_data_deprecated = all_resource_data[current_base_directory_name]

    return producer_data, consumer_data, resource_data_deprecated, all_resource_data


def draw_graphs(brokers, base_directory_name, producer_data, consumer_data, resource_usage_data):
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

    print('drawing resource graph... ', end=' ')
    draw_cpu_usage_graph(brokers, base_directory_name, resource_usage_data)
    draw_memory_usage_graph(brokers, base_directory_name, resource_usage_data)
    draw_disk_iops_graph(brokers, base_directory_name, resource_usage_data)
    draw_iowait_graph(brokers, base_directory_name, resource_usage_data)
    draw_disk_throughput_graph(brokers, base_directory_name, resource_usage_data)
    print('done.')

    # draw_lag_graph(consumer_data)


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
    all_base_directory_names = [
        '1-1-1-AT_LEAST_ONCE',
        '1-1-1-AT_MOST_ONCE',
        '1-1-1-EXACTLY_ONCE',
        '1-3-3-AT_LEAST_ONCE',
        '5-3-3-AT_LEAST_ONCE',
        '2-2-2-AT_LEAST_ONCE',
        '4-4-4-AT_LEAST_ONCE',
        '8-8-8-AT_LEAST_ONCE'
    ]

    if delivery_mode == 'EXACTLY_ONCE':
        if ('rabbitmq' in VISUALIZATION_BROKERS):
            VISUALIZATION_BROKERS.remove('rabbitmq')

    print('reading data...')
    producer_data, consumer_data, resource_data, all_resource_data = read_data(
        VISUALIZATION_BROKERS, base_directory_name, all_base_directory_names
    )
    print('successfully read data.')

    initialize_directory(base_directory_name)

    print('drawing graphs...')
    draw_graphs(VISUALIZATION_BROKERS, base_directory_name, producer_data, consumer_data, resource_data)
    print('done')
    print('drawing comparison resource graphs... ')
    draw_comparison_resource_graphs(VISUALIZATION_BROKERS, all_resource_data)
    print('done')

    print('successfully drew graphs.')

    print('visualization done')


if __name__ == '__main__':
    main()
