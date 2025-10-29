import requests
import pandas as pd
from datetime import datetime
import time
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import glob
import os


load_dotenv()
load_dotenv('../.env')

VISUALIZATION_BROKERS = os.getenv('VISUALIZATION_BROKERS').split(',')
if os.getenv('DELIVERY_MODE') == 'EXACTLY_ONCE':
    if ('rabbitmq' in VISUALIZATION_BROKERS):
        VISUALIZATION_BROKERS.remove('rabbitmq')
VISUALIZATION_RESULTS_DIR = os.getenv('VISUALIZATION_RESULTS_DIR')
PRODUCER_RESULT_CSV_FILENAME = os.getenv('PRODUCER_RESULT_CSV_FILENAME')


def read_benchmark_time(broker, base_directory_name):
    base_dir = os.path.join(os.path.curdir, os.pardir, broker, "results", base_directory_name, "producer")
    pattern = os.path.join(base_dir, f"benchmark_time-*.txt")
    file_list = glob.glob(pattern)
    if not file_list:
        raise FileNotFoundError(f"Cannot find file '{pattern}'")
    filename = file_list[0]

    with open(filename, mode='r') as txt_file:
        start_time, end_time = map(float, txt_file.readline().split(','))
    
    return start_time, end_time


def _read_and_concat_data(broker, base_directory_name, isProducer):
    target_dir_name = "producer" if isProducer else "consumer"
    target_pattern = 'producer_metrics-*.csv' if isProducer else 'consumer_metrics-*.csv'

    base_dir = os.path.join(os.path.curdir, os.pardir, broker, 'results', base_directory_name, target_dir_name)
    pattern = os.path.join(base_dir, target_pattern)
    file_list = glob.glob(pattern)

    dataframes = [pd.read_csv(file) for file in file_list]
    result = pd.concat(dataframes)

    result.sort_values(by="timestamp", inplace=True)

    return result


def read_and_concat_producer_data(broker, base_directory_name):
    return _read_and_concat_data(broker, base_directory_name, True)


def read_and_concat_consumer_data(broker, base_directory_name):
    return _read_and_concat_data(broker, base_directory_name, False)


def _filter_by_start_time_and_end_time(df, start_time, end_time):
    filtered_df = df[(start_time <= df['timestamp'] ) & (df['timestamp'] <= end_time)]

    return filtered_df


def _update_timestamp(df):
    min_value = df['timestamp'].min()
    df['timestamp'] = df['timestamp'] - min_value

    return df


def read_producer_data(broker, base_directory_name, start_time, end_time):
    result = read_and_concat_producer_data(broker, base_directory_name)
    result = _filter_by_start_time_and_end_time(result, start_time, end_time)
    result = _update_timestamp(result)

    result['timestamp'] = pd.to_datetime(result['timestamp'], unit='s')
    result['throughput'] = result['message_size']
    result['processing_time'] = result['processing_time']

    return result


def read_consumer_data(broker, base_directory_name, start_time, end_time):
    result = read_and_concat_consumer_data(broker, base_directory_name)
    result = _filter_by_start_time_and_end_time(result, start_time, end_time)
    result = _update_timestamp(result)

    result['timestamp'] = pd.to_datetime(result['timestamp'], unit='s')
    result['throughput'] = result['message_size']
    result['processing_time'] = result['processing_time']
    result['latency'] = result['latency']
    result['lag'] = result['lag']

    return result


# def read_cpu_usage_data(broker):
#     filename = f'../{broker}/cpu_usage.csv'
#     cpu_df = pd.read_csv(filename, sep=",")
#     cpu_df['timestamp'] = pd.to_datetime(cpu_df['Time'])
#     cpu_df['cpu_usage'] = (
#         cpu_df["Busy System"].str.rstrip('%').astype(float) +
#         cpu_df["Busy User"].str.rstrip('%').astype(float) +
#         cpu_df["Busy Iowait"].str.rstrip('%').astype(float) +
#         cpu_df["Busy IRQs"].str.rstrip('%').astype(float) +
#         cpu_df["Busy Other"].str.rstrip('%').astype(float)
#     )

#     return cpu_df


# def read_memory_usage_data(broker):
#     filename = f'../{broker}/memory_usage.csv'
#     memory_df = pd.read_csv(filename, sep=",")
#     memory_df['timestamp'] = pd.to_datetime(memory_df['Time'])
#     memory_df['RAM Used'] = memory_df['RAM Used'].apply(
#         lambda x: float(x.rstrip(" MiB")) / 1024 if "MiB" in x else float(x.rstrip(" GiB"))
#     )
#     memory_df['RAM Cache + Buffer'] = memory_df['RAM Cache + Buffer'].apply(
#         lambda x: float(x.rstrip(" MiB")) / 1024 if "MiB" in x else float(x.rstrip(" GiB"))
#     )
#     memory_df['memory_usage'] = memory_df['RAM Used'] + memory_df['RAM Cache + Buffer']

#     return memory_df


def fetch_prometheus_data(query, start_time_ts, end_time_ts):
    api_url = f'http://localhost:9090/api/v1/query_range'
    
    params = {
        'query': query,
        'start': start_time_ts,
        'end': end_time_ts,
        'step': '60s'
    }
    
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status() 
        data = response.json()
        
        if data['status'] == 'success' and data['data']['result']:
            result = data['data']['result'][0]['values'] 
            
            df = pd.DataFrame(result, columns=['timestamp', 'value'])
            
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
            
            df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Seoul')
            
            df['value'] = pd.to_numeric(df['value'])
            df = df.set_index('timestamp')
            return df[['value']]
        else:
            print(f"Data not found. query: {query}")
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from Prometheus: {e}")
        return pd.DataFrame()


def read_cpu_usage_data(start_ts, end_ts):
    cpu_queries = {
        'Busy User': 'rate(node_cpu_seconds_total{mode="user"}[1m]) * 100',
        'Busy System': 'rate(node_cpu_seconds_total{mode="system"}[1m]) * 100',
        'Busy Iowait': 'rate(node_cpu_seconds_total{mode="iowait"}[1m]) * 100',
        'Busy Other': 'rate(node_cpu_seconds_total{mode=~"nice|irq|softirq|steal|guest.*"}[1m]) * 100', 
        'Idle': 'rate(node_cpu_seconds_total{mode="idle"}[1m]) * 100'
    }
    
    cpu_df = pd.DataFrame()
    for label, query in cpu_queries.items():
        data = fetch_prometheus_data(query, start_ts, end_ts)
        if not data.empty:
            cpu_df[label] = data['value']
    
    assert not cpu_df.empty

    print(cpu_df)

    start_time = cpu_df.index[0]
    time_delta_index = cpu_df.index - start_time
    elapsed_minutes = time_delta_index.total_seconds() / 60
    cpu_df.index = elapsed_minutes

    return cpu_df


def read_memory_usage_data(start_ts, end_ts):
    total_mem_df = fetch_prometheus_data('node_memory_MemTotal_bytes', start_ts, end_ts)
    assert not total_mem_df.empty

    total_mem = total_mem_df['value'].iloc[0] 

    mem_queries = {
        'Used': 'node_memory_MemTotal_bytes - node_memory_MemFree_bytes - node_memory_Buffers_bytes - node_memory_Cached_bytes',
        'Cache + Buffer': 'node_memory_Buffers_bytes + node_memory_Cached_bytes',
        'Free': 'node_memory_MemFree_bytes'
    }
    
    mem_df = pd.DataFrame()
    for label, query in mem_queries.items():
        data = fetch_prometheus_data(query, start_ts, end_ts)
        if not data.empty:
            mem_df[label] = (data['value'] / total_mem) * 100
            
    assert not mem_df.empty
    
    start_time = mem_df.index[0]
    time_delta_index = mem_df.index - start_time
    elapsed_minutes = time_delta_index.total_seconds() / 60
    mem_df.index = elapsed_minutes

    return mem_df


def read_disk_iops_data(start_ts, end_ts):
    disk_queries = {
        'Read IOps': 'sum(rate(node_disk_reads_completed_total[1m]))',
        'Write IOps': 'sum(rate(node_disk_writes_completed_total[1m]))'
    }
    
    disk_df = pd.DataFrame()
    for label, query in disk_queries.items():
        data = fetch_prometheus_data(query, start_ts, end_ts)
        if not data.empty:
            disk_df[label] = data['value']
            
    assert not disk_df.empty

    start_time = disk_df.index[0]
    time_delta_index = disk_df.index - start_time
    elapsed_minutes = time_delta_index.total_seconds() / 60
    disk_df.index = elapsed_minutes

    return disk_df


def save_plot(fig, base_directory_name, filename):
    filepath = os.path.join(VISUALIZATION_RESULTS_DIR, base_directory_name, f'{filename}-{datetime.now().strftime("%m%d_%H%M%S")}.png')
    fig.savefig(filepath, bbox_inches='tight')
    plt.close(fig)
