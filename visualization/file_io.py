import pandas as pd
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import glob
import os


load_dotenv()

VISUALIZATION_BROKERS = os.getenv('VISUALIZATION_BROKERS').split(',')

VISUALIZATION_RESULTS_DIR = os.getenv('VISUALIZATION_RESULTS_DIR')
RESULT_BENCHMARK_TIME_FILENAME = os.getenv('RESULT_BENCHMARK_TIME_FILENAME')
PRODUCER_RESULT_CSV_FILENAME = os.getenv('PRODUCER_RESULT_CSV_FILENAME')


def read_benchmark_time(broker):
    filename = os.path.join(os.path.curdir, f"../{broker}/{RESULT_BENCHMARK_TIME_FILENAME}-1.txt")
    with open(filename, mode='r') as txt_file:
        start_time, end_time = map(float, txt_file.readline().split(','))
    
    return start_time, end_time



def read_and_concat_data(broker):
    filename = f"../{broker}/{PRODUCER_RESULT_CSV_FILENAME}-*.csv"
    file_paths = glob.glob(filename)

    dataframes = [pd.read_csv(file) for file in file_paths]
    result = pd.concat(dataframes)

    result.sort_values(by="timestamp", inplace=True)

    return result


def _filter_by_start_time_and_end_time(df, start_time, end_time):
    filtered_df = df[(start_time <= df['timestamp'] ) & (df['timestamp'] <= end_time)]
    

    return filtered_df


def _update_timestamp(df):
    min_value = df['timestamp'].min()
    df['timestamp'] = df['timestamp'] - min_value

    return df


def read_producer_data(broker, start_time, end_time):
    result = read_and_concat_data(broker)
    result = _filter_by_start_time_and_end_time(result, start_time, end_time)
    result = _update_timestamp(result)

    result['timestamp'] = pd.to_datetime(result['timestamp'], unit='s')
    result['throughput'] = result['message_size']
    result['processing_time'] = result['processing_time']

    return result


def read_consumer_data(broker, start_time, end_time):
    filename = os.path.join(os.path.curdir, f"../{broker}/consumer_metrics.csv")
    result = pd.read_csv(filename)
    result = _filter_by_start_time_and_end_time(result, start_time, end_time)
    result = _update_timestamp(result)

    result['timestamp'] = pd.to_datetime(result['timestamp'], unit='s')
    result['throughput'] = result['message_size']
    result['processing_time'] = result['processing_time']
    result['latency'] = result['latency']
    result['lag'] = result['lag']

    return result


def read_cpu_usage_data(broker):
    filename = f'../{broker}/cpu_usage.csv'
    cpu_df = pd.read_csv(filename, sep=",")
    cpu_df['timestamp'] = pd.to_datetime(cpu_df['Time'])
    cpu_df['cpu_usage'] = (
        cpu_df["Busy System"].str.rstrip('%').astype(float) +
        cpu_df["Busy User"].str.rstrip('%').astype(float) +
        cpu_df["Busy Iowait"].str.rstrip('%').astype(float) +
        cpu_df["Busy IRQs"].str.rstrip('%').astype(float) +
        cpu_df["Busy Other"].str.rstrip('%').astype(float)
    )

    return cpu_df


def read_memory_usage_data(broker):
    filename = f'../{broker}/memory_usage.csv'
    memory_df = pd.read_csv(filename, sep=",")
    memory_df['timestamp'] = pd.to_datetime(memory_df['Time'])
    memory_df['RAM Used'] = memory_df['RAM Used'].apply(
        lambda x: float(x.rstrip(" MiB")) / 1024 if "MiB" in x else float(x.rstrip(" GiB"))
    )
    memory_df['RAM Cache + Buffer'] = memory_df['RAM Cache + Buffer'].apply(
        lambda x: float(x.rstrip(" MiB")) / 1024 if "MiB" in x else float(x.rstrip(" GiB"))
    )
    memory_df['memory_usage'] = memory_df['RAM Used'] + memory_df['RAM Cache + Buffer']

    return memory_df


graph_no = 0
def save_plot(fig, filename):
    global graph_no
    graph_no += 1
    fig.savefig(os.path.join(VISUALIZATION_RESULTS_DIR, f'{graph_no}_{filename}'), bbox_inches='tight')
    plt.close(fig)
