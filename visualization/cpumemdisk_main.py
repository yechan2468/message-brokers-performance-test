import os
import sys
from dotenv import load_dotenv
from visualize_cpumemdisk import *
from file_io import *
from util import *


load_dotenv()
load_dotenv('../.env')

VISUALIZATION_BROKERS = os.getenv('VISUALIZATION_BROKERS').split(',')
VISUALIZATION_RESULTS_DIR = os.getenv('VISUALIZATION_RESULTS_DIR')
BENCHMARK_WARMUP_MINUTES = float(os.getenv('BENCHMARK_WARMUP_MINUTES'))
BENCHMARK_DURATION_MINUTES = float(os.getenv('BENCHMARK_DURATION_MINUTES'))

COMPARISON_GROUPS = {
    'delivery_mode': {
        'directories': ['1-1-1-AT_LEAST_ONCE', '1-1-1-AT_MOST_ONCE', '1-1-1-EXACTLY_ONCE'],
        'labels': {
            '1-1-1-AT_LEAST_ONCE': 'At Least Once',
            '1-1-1-AT_MOST_ONCE': 'At Most Once',
            '1-1-1-EXACTLY_ONCE': 'Exactly Once'
        },
    },
    'producer_consumer_count': {
        'directories': ['1-1-1-AT_LEAST_ONCE', '1-3-3-AT_LEAST_ONCE', '5-3-3-AT_LEAST_ONCE'],
        'labels': {
            '1-1-1-AT_LEAST_ONCE': '#prod=1, #cons=1',
            '1-3-3-AT_LEAST_ONCE': '#prod=1, #cons=3',
            '5-3-3-AT_LEAST_ONCE': '#prod=5, #cons=3'
        },
    },
    'partition_count': {
        'directories': ['1-1-1-AT_LEAST_ONCE', '2-2-2-AT_LEAST_ONCE', '4-4-4-AT_LEAST_ONCE', '8-8-8-AT_LEAST_ONCE'],
        'labels': {
            '1-1-1-AT_LEAST_ONCE': '#partition=1',
            '2-2-2-AT_LEAST_ONCE': '#partition=2',
            '4-4-4-AT_LEAST_ONCE': '#partition=4',
            '8-8-8-AT_LEAST_ONCE': '#partition=8'
        },
    }
}


def read_resource_data(brokers, all_base_directory_names):
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

    return all_resource_data

def main():
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

    for group_names in COMPARISON_GROUPS:
        for metric_type in ['cpu', 'memory', 'iowait', 'iops', 'throughput']:
            initialize_directory(f'{group_names}/{metric_type}')

    all_resource_data = read_resource_data(VISUALIZATION_BROKERS, all_base_directory_names)

    print('drawing comparison resource graphs... ')
    draw_comparison_resource_graphs(VISUALIZATION_BROKERS, all_resource_data, COMPARISON_GROUPS)
    print('done')

if __name__ == '__main__':
    main()
