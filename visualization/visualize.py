import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from file_io import save_plot


load_dotenv()

VISUALIZATION_BROKERS = os.getenv('VISUALIZATION_BROKERS').split(',')
VISUALIZATION_RESULTS_DIR = os.getenv('VISUALIZATION_RESULTS_DIR')


def draw_producer_throughput_graph(base_directory_name, producer_data):
    message_size = 0
    throughputs = []
    for broker in VISUALIZATION_BROKERS:
        prod_df = producer_data[broker]
        message_size = prod_df['message_size'].iloc[0]
        number_of_messages = len(prod_df)
        # total_message_size = message_size * number_of_messages

        total_time = (prod_df['timestamp'].iloc[-1] - prod_df['timestamp'].iloc[0]).total_seconds()

        throughput = number_of_messages / total_time
        throughputs.append(throughput)
    
    x_labels = [f"{VISUALIZATION_BROKERS[i]}" for i in range(len(throughputs))]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(x_labels, throughputs, color='skyblue', edgecolor='black')

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, height, f"{height:.0f}", 
                ha='center', va='bottom', fontsize=10, color='black')

    ax.set_title(f"Producer Throughput", fontsize=14)
    ax.set_ylabel("Throughput", fontsize=12)
    ax.set_xlabel("Brokers", fontsize=12)
    ax.set_ylim(0, max(throughputs) * 1.2)

    save_plot(fig, base_directory_name, f"producer_throughput.png")


def draw_consumer_throughput_graph(base_directory_name, consumer_data):
    message_size = 0
    throughputs = []
    for broker in VISUALIZATION_BROKERS:
        cons_df = consumer_data[broker]
        message_size = cons_df['message_size'].iloc[0]
        number_of_messages = len(cons_df)
        # total_message_size = message_size * number_of_messages

        total_time = (cons_df['timestamp'].iloc[-1] - cons_df['timestamp'].iloc[0]).total_seconds()

        throughput = number_of_messages / total_time
        throughputs.append(throughput)
    
    x_labels = [f"{VISUALIZATION_BROKERS[i]}" for i in range(len(throughputs))]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(x_labels, throughputs, color='skyblue', edgecolor='black')

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, height, f"{height:.0f}", 
                ha='center', va='bottom', fontsize=10, color='black')

    ax.set_title(f"Consumer Throughput", fontsize=14)
    ax.set_ylabel("Throughput", fontsize=12)
    ax.set_xlabel("Brokers", fontsize=12)
    ax.set_ylim(0, max(throughputs) * 1.2)

    save_plot(fig, base_directory_name, f"consumer_throughput.png")


def _draw_broker_throughput_byterate_boxplots(base_directory_name, producer_data, consumer_data):
    for broker in VISUALIZATION_BROKERS:
        prod_df = producer_data[broker]
        cons_df = consumer_data[broker]

        combined_data = pd.DataFrame({
            "Type": (
                ["Producer Throughput"] * len(prod_df) +
                ["Producer Byterate"] * len(prod_df) +
                ["Consumer Throughput"] * len(cons_df) +
                ["Consumer Byterate"] * len(cons_df)
            ),
            "Value": pd.concat([
                prod_df['throughput'], prod_df['byterate'],
                cons_df['throughput'], cons_df['byterate']
            ]).reset_index(drop=True)
        })

        fig, ax = plt.subplots(1, 1, figsize=(16, 6))
        sns.boxplot(data=combined_data, x="Type", y="Value", ax=ax, showfliers=False)
        ax.set_title(f"{broker} Throughput and Byterate (Box Plot)")
        ax.set_ylabel("Value")
        ax.set_xlabel("")

        for i, category in enumerate(combined_data["Type"].unique()):
            stats = combined_data[combined_data["Type"] == category]["Value"].describe()
            ax.annotate(
                f"Mean: {stats['mean']:.4f}\nMedian: {stats['50%']:.4f}\nStddev: {stats['std']:.4f}\n"
                f"Max: {stats['max']:.4f}\nMin: {stats['min']:.4f}",
                xy=(i, stats['75%']),
                xytext=(0, 10),
                textcoords='offset points',
                ha='center',
                fontsize=8
            )

        plt.tight_layout()
        save_plot(fig, base_directory_name, f"{broker}_throughput_byterate_boxplot.png")


def draw_consumer_throughput_byterate_graph(base_directory_name, consumer_data):
    for broker in VISUALIZATION_BROKERS:
        cons_df = consumer_data[broker]

        throughput_data = pd.DataFrame({
            "Type": ["Consumer Throughput"] * len(cons_df),
            "Throughput": cons_df['throughput']
        }).reset_index(drop=True)

        byterate_data = pd.DataFrame({
            "Type": ["Consumer Byterate"] * len(cons_df),
            "Byterate": cons_df['byterate']
        }).reset_index(drop=True)

        fig, axes = plt.subplots(1, 2, figsize=(16, 6), sharey=False)

        sns.boxplot(data=throughput_data, x="Type", y="Throughput", ax=axes[0], showfliers=False)
        axes[0].set_title(f"{broker} Consumer Throughput (Box Plot)")
        axes[0].set_ylabel("Throughput (messages/s)")
        axes[0].set_xlabel("")
        stats = throughput_data["Throughput"].describe()
        axes[0].annotate(
            f"Mean: {stats['mean']:.4f}\nMedian: {stats['50%']:.4f}\nStddev: {stats['std']:.4f}\nMax: {stats['max']:.4f}\nMin: {stats['min']:.4f}",
            xy=(0, stats['75%']),
            xytext=(0, 10),
            textcoords='offset points',
            ha='center',
            fontsize=8
        )

        sns.boxplot(data=byterate_data, x="Type", y="Byterate", ax=axes[1], showfliers=False)
        axes[1].set_title(f"{broker} Consumer Byterate (Box Plot)")
        axes[1].set_ylabel("Byterate (bytes/s)")
        axes[1].set_xlabel("")
        stats = byterate_data["Byterate"].describe()
        axes[1].annotate(
            f"Mean: {stats['mean']:.4f}\nMedian: {stats['50%']:.4f}\nStddev: {stats['std']:.4f}\nMax: {stats['max']:.4f}\nMin: {stats['min']:.4f}",
            xy=(0, stats['75%']),
            xytext=(0, 10),
            textcoords='offset points',
            ha='center',
            fontsize=8
        )

        plt.tight_layout()
        save_plot(fig, base_directory_name, f"{broker}_consumer_throughput_byterate_boxplot.png")


def _draw_producer_throughput_byterate_boxplots(base_directory_name, producer_data):
    throughput_data = []
    byterate_data = []

    for broker in VISUALIZATION_BROKERS:
        df = producer_data[broker]

        throughput_data.append(pd.DataFrame({
            "Broker": [broker] * len(df),
            "Value": df['throughput'],
            "Metric": ["Producer Throughput"] * len(df)
        }))

        byterate_data.append(pd.DataFrame({
            "Broker": [broker] * len(df),
            "Value": df['byterate'],
            "Metric": ["Producer Byterate"] * len(df)
        }))

    throughput_data = pd.concat(throughput_data).reset_index(drop=True)
    byterate_data = pd.concat(byterate_data).reset_index(drop=True)

    metrics = [
        ("Producer Throughput Comparison", throughput_data),
        ("Producer Byterate Comparison", byterate_data),
    ]

    for title, data in metrics:
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.boxplot(data=data, x="Broker", y="Value", ax=ax, showfliers=False)
        ax.set_title(title)
        ax.set_ylabel("Value")
        ax.set_xlabel("Broker")

        for i, broker in enumerate(data["Broker"].unique()):
            stats = data[data["Broker"] == broker]["Value"].describe()
            ax.annotate(
                f"Mean: {stats['mean']:.4f}\nMedian: {stats['50%']:.4f}\nStddev: {stats['std']:.4f}\n"
                f"Max: {stats['max']:.4f}\nMin: {stats['min']:.4f}",
                xy=(i, stats['75%']),
                xytext=(0, 10),
                textcoords='offset points',
                ha='center',
                fontsize=8
            )

        plt.tight_layout()
        save_plot(fig, base_directory_name, f"{title.replace(' ', '_').lower()}.png")


def _draw_consumer_throughput_byterate_boxplots(base_directory_name, consumer_data):
    throughput_data = []
    byterate_data = []

    for broker in VISUALIZATION_BROKERS:
        df = consumer_data[broker]

        throughput_data.append(pd.DataFrame({
            "Broker": [broker] * len(df),
            "Value": df['throughput'],
            "Metric": ["Consumer Throughput"] * len(df)
        }))

        byterate_data.append(pd.DataFrame({
            "Broker": [broker] * len(df),
            "Value": df['byterate'],
            "Metric": ["Consumer Byterate"] * len(df)
        }))

    throughput_data = pd.concat(throughput_data).reset_index(drop=True)
    byterate_data = pd.concat(byterate_data).reset_index(drop=True)

    metrics = [
        ("Consumer Throughput Comparison", throughput_data),
        ("Consumer Byterate Comparison", byterate_data),
    ]

    for title, data in metrics:
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.boxplot(data=data, x="Broker", y="Value", ax=ax, showfliers=False)
        ax.set_title(title)
        ax.set_ylabel("Value")
        ax.set_xlabel("Broker")

        for i, broker in enumerate(data["Broker"].unique()):
            stats = data[data["Broker"] == broker]["Value"].describe()
            ax.annotate(
                f"Mean: {stats['mean']:.4f}\nMedian: {stats['50%']:.4f}\nStddev: {stats['std']:.4f}\n"
                f"Max: {stats['max']:.4f}\nMin: {stats['min']:.4f}",
                xy=(i, stats['75%']),
                xytext=(0, 10),
                textcoords='offset points',
                ha='center',
                fontsize=8
            )

        plt.tight_layout()
        save_plot(fig, base_directory_name, f"{title.replace(' ', '_').lower()}.png")



def draw_latency_boxplot(base_directory_name, consumer_data):
    fig, ax = plt.subplots(figsize=(10, 6))
    latency_data = pd.concat(
        [consumer_data[br][['latency']].assign(Broker=br) for br in consumer_data]
    )
    sns.boxplot(data=latency_data, x='Broker', y='latency', ax=ax, fliersize=2)

    for broker in consumer_data:
        broker_data = consumer_data[broker]['latency']
        stats = broker_data.describe()
        ax.annotate(f"Mean: {stats['mean']:.2f}\nMedian: {stats['50%']:.2f}\nStddev: {stats['std']:.2f}\nMax: {stats['max']:.2f}\nMin: {stats['min']:.2f}", 
                    xy=(broker, stats['75%']), xytext=(0, 20), 
                    textcoords='offset points', ha='center', fontsize=8)

    ax.set_title("Latency Distribution")
    ax.set_xlabel("Broker")
    ax.set_ylabel("Latency (seconds)")
    plt.tight_layout()
    save_plot(fig, base_directory_name, 'latency.png')


def draw_latency_histogram(base_directory_name, consumer_data):
    fig, axes = plt.subplots(len(consumer_data), 1, figsize=(10, 4 * len(consumer_data)), sharex=True)

    for idx, (broker, data) in enumerate(consumer_data.items()):
        ax = axes[idx] if len(consumer_data) > 1 else axes
        sns.histplot(data['latency'], kde=True, stat='probability', bins=30, ax=ax)
        ax.set_title(f"{broker} Latency PDF")
        ax.set_xlabel("Latency (seconds)")
        ax.set_ylabel("Probability")

    plt.tight_layout()
    save_plot(fig, base_directory_name, 'latency_histogram.png')


def draw_lag_graph(base_directory_name, consumer_data):
    fig, ax = plt.subplots(figsize=(12, 6))
    for broker in consumer_data:
        ax.plot(consumer_data[broker]['timestamp'], consumer_data[broker]['lag'], label=broker)

    ax.set_title("Consumer Lag Over Time")
    ax.set_xlabel("Time")
    ax.set_ylabel("Lag")
    ax.legend()
    plt.tight_layout()
    save_plot(fig, base_directory_name, 'lag.png')


def draw_cpu_usage_graph(base_directory_name, resource_usage_data):
    for broker in VISUALIZATION_BROKERS:
        data = resource_usage_data[broker]['cpu']
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(data['timestamp'], data['cpu_usage'], label="CPU Usage")
        ax.set_title("CPU Usage Over Time")
        ax.set_xlabel("Time")
        ax.set_ylabel("CPU Usage (%)")
        ax.legend()
        plt.tight_layout()
        save_plot(fig, base_directory_name, f'{broker}_cpu_usage.png')


def draw_memory_usage_graph(base_directory_name, resource_usage_data):
    for broker in VISUALIZATION_BROKERS:
        data = resource_usage_data[broker]['memory']
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(data['timestamp'], data['memory_usage'], label="Memory Usage (GiB)")
        ax.set_title("Memory Usage Over Time")
        ax.set_xlabel("Time")
        ax.set_ylabel("Memory Usage (GiB)")
        ax.legend()
        plt.tight_layout()
        save_plot(fig, base_directory_name, f'{broker}_memory_usage.png')


def draw_producer_processing_time_graph(base_directory_name, producer_data):
    return _draw_processing_time_graph(base_directory_name, producer_data, True)


def draw_consumer_processing_time_graph(base_directory_name, consumer_data):
    return _draw_processing_time_graph(base_directory_name, consumer_data, False)


def _draw_processing_time_graph(base_directory_name, data, is_producer):
    plot_data = [data[d]['processing_time'] for d in data]
    
    stats_list = []
    overall_max_for_limit = 0
    for broker_name in VISUALIZATION_BROKERS:
        series = pd.Series(data[broker_name]['processing_time'])
        stats = series.describe()
        stats_list.append(stats)
        
        current_max_y = stats['max']
        if current_max_y > overall_max_for_limit:
            overall_max_for_limit = current_max_y
            
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.boxplot(
        plot_data,
        labels=VISUALIZATION_BROKERS,
        patch_artist=True,
        boxprops=dict(facecolor='skyblue', color='black'),
        medianprops=dict(color='black', linewidth=1),
        whiskerprops=dict(color='black', linewidth=1.5),
        capprops=dict(color='black', linewidth=1.5),
        showfliers=False
    )

    for i, stats in enumerate(stats_list):
        annotation_text = (
            f"Mean: {stats['mean']:.1f} μs\n"
            f"Median: {stats['50%']:.1f} μs\n"
            f"Stddev: {stats['std']:.1f} μs\n"
            f"Max: {stats['max']:.1f} μs\n"
            f"Min: {stats['min']:.1f} μs"
        )
        
        base_y = stats['75%'] 
        
        ax.annotate(
            annotation_text, 
            xy=(i + 1, base_y), 
            xytext=(0, 5),
            textcoords='offset points', 
            ha='center', 
            fontsize=8,
            # bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.7)
        )

    ax.set_yscale('log')
    
    if overall_max_for_limit > 0:
        ax.set_ylim(ymax=overall_max_for_limit * 1.05) 
    
    ax.set_title(f"{'Producer' if is_producer else 'Consumer'} Processing Time Distribution Across Brokers", fontsize=14)
    ax.set_ylabel("Processing Time (μs, log10 scale)", fontsize=12)
    ax.set_xlabel("Brokers", fontsize=12)

    save_plot(fig, base_directory_name, f'{"producer" if is_producer else "consumer"}_processing_time_boxplot')
