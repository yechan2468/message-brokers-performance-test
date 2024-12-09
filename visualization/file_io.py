import pandas as pd
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import os


load_dotenv()

BROKERS = os.getenv('BROKERS').split(',')
RESULTS_DIR = os.getenv('RESULTS_DIR')


def read_producer_data(broker):
    filename = os.path.join(os.path.curdir, f"../{broker}/producer_metrics.csv")
    result = pd.read_csv(filename)

    result['timestamp'] = pd.to_datetime(result['timestamp'], unit='s')
    result['throughput'] = result['message_size']
    result['byterate'] = result['byte_size'].diff().replace(0, None).ffill()

    return result


def read_consumer_data(broker):
    filename = os.path.join(os.path.curdir, f"../{broker}/consumer_metrics.csv")
    result = pd.read_csv(filename)

    result['timestamp'] = pd.to_datetime(result['timestamp'], unit='s')
    result['throughput'] = result['message_size']
    result['byterate'] = result['byte_size'].diff().replace(0, None).ffill()
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
    fig.savefig(os.path.join(RESULTS_DIR, f'{graph_no}_{filename}'), bbox_inches='tight')
    plt.close(fig)
