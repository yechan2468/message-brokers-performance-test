import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


BROKERS = ["kafka", "redpanda"]
RESULTS_DIR = "results/"


def read_data():
    producer_data = {}
    consumer_data = {}
    resource_usage_data = {}

    for broker in BROKERS:
        producer_data[broker] = read_producer_data(broker)
        consumer_data[broker] = read_consumer_data(broker)

        resource_usage_data[broker] = {}
        resource_usage_data[broker]['cpu'] = read_cpu_usage_data(broker)
        resource_usage_data[broker]['memory'] = read_memory_usage_data(broker)

    return producer_data, consumer_data, resource_usage_data


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


def initialize_directory():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    for file in os.listdir(RESULTS_DIR):
        os.remove(os.path.join(RESULTS_DIR, file))


graph_no = 0
def save_plot(fig, filename):
    global graph_no
    graph_no += 1
    fig.savefig(os.path.join(RESULTS_DIR, f'{graph_no}_{filename}'), bbox_inches='tight')
    plt.close(fig)


def draw_throughput_byterate_graph(producer_data, consumer_data):
    for broker in BROKERS:
        prod_df = producer_data[broker]
        cons_df = consumer_data[broker]
    
        fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
        axes[0].plot(prod_df['timestamp'], prod_df['throughput'], label="Producer Throughput")
        axes[0].plot(cons_df['timestamp'], cons_df['throughput'], label="Consumer Throughput")
        axes[0].set_ylabel("Throughput (messages/s)")
        axes[0].legend()
    
        axes[1].plot(prod_df['timestamp'], prod_df['byterate'], label="Producer Byterate")
        axes[1].plot(cons_df['timestamp'], cons_df['byterate'], label="Consumer Byterate")
        axes[1].set_ylabel("Byterate (bytes/s)")
        axes[1].legend()
    
        fig.suptitle(f"{broker} Throughput and Byterate")
        save_plot(fig, f"{broker}_throughput_byterate.png")


def draw_producer_throughput_comparison_graph(data):
    fig, ax = plt.subplots(figsize=(12, 6))
    for broker in BROKERS:
        ax.plot(data[broker]['timestamp'], data[broker]['throughput'], label=broker)
    ax.set_title("Producer Throughput Comparison")
    ax.set_ylabel("Throughput (messages/s)")
    ax.legend()
    save_plot(fig, "producer_throughput_comparison.png")


def draw_producer_byterate_comparison_graph(data):
    fig, ax = plt.subplots(figsize=(12, 6))
    for broker in BROKERS:
        ax.plot(data[broker]['timestamp'], data[broker]['byterate'], label=broker)
    ax.set_title("Producer Byterate Comparison")
    ax.set_ylabel("Byterate (bytes/s)")
    ax.legend()
    save_plot(fig, "producer_byterate_comparison.png")


def draw_consumer_throughput_comparison_graph(data):
    fig, ax = plt.subplots(figsize=(12, 6))
    for broker in BROKERS:
        ax.plot(data[broker]['timestamp'], data[broker]['throughput'], label=broker)
    ax.set_title("Producer Throughput Comparison")
    ax.set_ylabel("Throughput (messages/s)")
    ax.legend()
    save_plot(fig, "consumer_throughput_comparison.png")


def draw_consumer_byterate_comparison_graph(data):
    fig, ax = plt.subplots(figsize=(12, 6))
    for broker in BROKERS:
        ax.plot(data[broker]['timestamp'], data[broker]['byterate'], label=broker)
    ax.set_title("Producer Byterate Comparison")
    ax.set_ylabel("Byterate (bytes/s)")
    ax.legend()
    save_plot(fig, "consumer_byterate_comparison.png")


def draw_latency_graph(consumer_data):
    fig, ax = plt.subplots(figsize=(10, 6))
    latency_data = pd.concat(
        [consumer_data[br][['latency']].assign(Broker=br) for br in consumer_data]
    )
    sns.boxplot(data=latency_data, x='Broker', y='latency', ax=ax, fliersize=2)

    for broker in consumer_data:
        broker_data = consumer_data[broker]['latency']
        stats = broker_data.describe()
        ax.annotate(f"Mean: {stats['mean']:.4f}\nMedian: {stats['50%']:.4f}\nStddev: {stats['std']:.4f}]\nMax: {stats['max']:.4f}\nMin: {stats['min']:.4f}", 
                    xy=(broker, stats['75%']), xytext=(0, 20), 
                    textcoords='offset points', ha='center', fontsize=8)

    ax.set_title("Latency Distribution")
    ax.set_xlabel("Broker")
    ax.set_ylabel("Latency (seconds)")
    plt.tight_layout()
    save_plot(fig, 'latency.png')


def draw_latency_histogram(consumer_data):
    fig, axes = plt.subplots(len(consumer_data), 1, figsize=(10, 4 * len(consumer_data)), sharex=True)

    for idx, (broker, data) in enumerate(consumer_data.items()):
        ax = axes[idx] if len(consumer_data) > 1 else axes
        sns.histplot(data['latency'], kde=True, stat='probability', bins=30, ax=ax)
        ax.set_title(f"{broker} Latency PDF")
        ax.set_xlabel("Latency (seconds)")
        ax.set_ylabel("Probability")

    plt.tight_layout()
    save_plot(fig, 'latency_histogram.png')


def draw_lag_graph(consumer_data):
    fig, ax = plt.subplots(figsize=(12, 6))
    for broker in consumer_data:
        ax.plot(consumer_data[broker]['timestamp'], consumer_data[broker]['lag'], label=broker)

    ax.set_title("Consumer Lag Over Time")
    ax.set_xlabel("Time")
    ax.set_ylabel("Lag")
    ax.legend()
    plt.tight_layout()
    save_plot(fig, 'lag.png')


def draw_cpu_usage_graph(resource_usage_data):
    for broker in BROKERS:
        data = resource_usage_data[broker]['cpu']
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(data['timestamp'], data['cpu_usage'], label="CPU Usage")
        ax.set_title("CPU Usage Over Time")
        ax.set_xlabel("Time")
        ax.set_ylabel("CPU Usage (%)")
        ax.legend()
        plt.tight_layout()
        save_plot(fig, f'{broker}_cpu_usage.png')


def draw_memory_usage_graph(resource_usage_data):
    for broker in BROKERS:
        data = resource_usage_data[broker]['memory']
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(data['timestamp'], data['memory_usage'], label="Memory Usage (GiB)")
        ax.set_title("Memory Usage Over Time")
        ax.set_xlabel("Time")
        ax.set_ylabel("Memory Usage (GiB)")
        ax.legend()
        plt.tight_layout()
        save_plot(fig, f'{broker}_memory_usage.png')


def draw_graphs(producer_data, consumer_data, resource_usage_data):
    draw_throughput_byterate_graph(producer_data, consumer_data)

    draw_producer_throughput_comparison_graph(producer_data)
    draw_producer_byterate_comparison_graph(producer_data)
    draw_consumer_throughput_comparison_graph(consumer_data)
    draw_consumer_byterate_comparison_graph(consumer_data)

    draw_latency_graph(consumer_data)
    draw_latency_histogram(consumer_data)

    draw_lag_graph(consumer_data)

    draw_cpu_usage_graph(resource_usage_data)
    draw_memory_usage_graph(resource_usage_data)


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
