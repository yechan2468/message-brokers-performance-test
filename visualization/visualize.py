import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from file_io import save_plot


load_dotenv()

VISUALIZATION_BROKERS = os.getenv('VISUALIZATION_BROKERS').split(',') # deprecated. don't use
VISUALIZATION_RESULTS_DIR = os.getenv('VISUALIZATION_RESULTS_DIR')


def draw_producer_throughput_graph(brokers, base_directory_name, producer_data):
    message_size = 0
    throughputs = []
    for broker in brokers:
        prod_df = producer_data[broker]
        message_size = prod_df['message_size'].iloc[0]
        number_of_messages = len(prod_df)
        # total_message_size = message_size * number_of_messages

        total_time = (prod_df['timestamp'].iloc[-1] - prod_df['timestamp'].iloc[0]).total_seconds()

        throughput = number_of_messages / total_time
        throughputs.append(throughput)
    
    x_labels = [f"{brokers[i]}" for i in range(len(throughputs))]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(x_labels, throughputs, color='skyblue', edgecolor='black')

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, height, f"{height:.0f}", 
                ha='center', va='bottom', fontsize=10, color='black')

    ax.set_title(f"Producer Throughput", fontsize=14)
    ax.set_ylabel("Throughput (messages/sec)", fontsize=12)
    ax.set_xlabel("Brokers", fontsize=12)
    ax.set_ylim(0, max(throughputs) * 1.2)

    save_plot(fig, base_directory_name, f"producer_throughput")


def draw_consumer_throughput_graph(brokers, base_directory_name, consumer_data):
    message_size = 0
    throughputs = []
    for broker in brokers:
        cons_df = consumer_data[broker]
        message_size = cons_df['message_size'].iloc[0]
        number_of_messages = len(cons_df)
        # total_message_size = message_size * number_of_messages

        total_time = (cons_df['timestamp'].iloc[-1] - cons_df['timestamp'].iloc[0]).total_seconds()

        throughput = number_of_messages / total_time
        throughputs.append(throughput)
    
    x_labels = [f"{brokers[i]}" for i in range(len(throughputs))]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(x_labels, throughputs, color='skyblue', edgecolor='black')

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, height, f"{height:.0f}", 
                ha='center', va='bottom', fontsize=10, color='black')

    ax.set_title(f"Consumer Throughput", fontsize=14)
    ax.set_ylabel("Throughput (messages/sec)", fontsize=12)
    ax.set_xlabel("Brokers", fontsize=12)
    ax.set_ylim(0, max(throughputs) * 1.2)

    save_plot(fig, base_directory_name, f"consumer_throughput")


def draw_latency_boxplot(base_directory_name, consumer_data):
    fig, ax = plt.subplots(figsize=(10, 6))
    latency_data = pd.concat(
        [consumer_data[br][['latency']].assign(Broker=br) for br in consumer_data],
        ignore_index=True
    )
    sns.boxplot(data=latency_data, x='Broker', y='latency', ax=ax, fliersize=2)
    ax.set_yscale('log')

    for broker in consumer_data:
        broker_data = consumer_data[broker]['latency']

        stats = broker_data.describe()

        percentiles = broker_data.quantile([0.99, 0.999, 0.9999])
        p99 = percentiles[0.99]
        p999 = percentiles[0.999]
        p9999 = percentiles[0.9999]

        annotation_text = (
            f"Mean: {stats['mean']:.2f}\n"
            f"Median: {stats['50%']:.2f}\n"
            f"Stddev: {stats['std']:.2f}\n"
            f"Min: {stats['min']:.2f}\n"
            f"P99: {p99:.2f}\n"
            f"P99.9: {p999:.2f}\n"
            f"P99.99: {p9999:.2f}\n"
            f"Max: {stats['max']:.2f}"
        )

        ax.annotate(annotation_text, 
                    xy=(broker, stats['75%']), xytext=(35, 35), 
                    textcoords='offset points', ha='center', fontsize=8)

    ax.set_title("Latency Distribution")
    ax.set_xlabel("Broker")
    ax.set_ylabel("Latency (ms)")
    plt.tight_layout()
    save_plot(fig, base_directory_name, 'latency')


def draw_latency_histogram(base_directory_name, consumer_data):
    fig, axes = plt.subplots(len(consumer_data), 1, figsize=(10, 4 * len(consumer_data)), sharex=True)

    for idx, (broker, data) in enumerate(consumer_data.items()):
        ax = axes[idx] if len(consumer_data) > 1 else axes
        sns.histplot(data['latency'], kde=True, stat='probability', bins=30, ax=ax)

        ax.set_yscale('log')
        ax.set_title(f"{broker} Latency PDF")
        ax.set_xlabel("Latency (ms)")
        ax.set_ylabel("Probability")

    plt.tight_layout()
    save_plot(fig, base_directory_name, 'latency_histogram')


def draw_producer_processing_time_graph(brokers, base_directory_name, producer_data):
    return _draw_processing_time_graph(brokers, base_directory_name, producer_data, True)


def draw_consumer_processing_time_graph(brokers, base_directory_name, consumer_data):
    return _draw_processing_time_graph(brokers, base_directory_name, consumer_data, False)


def _draw_processing_time_graph(brokers, base_directory_name, data, is_producer):
    plot_data = [data[d]['processing_time'] for d in data]
    
    stats_list = []
    overall_max_for_limit = 0
    for broker_name in brokers:
        series = pd.Series(data[broker_name]['processing_time'])
        stats = series.describe()
        stats_list.append(stats)
        
        current_max_y = stats['max']
        if current_max_y > overall_max_for_limit:
            overall_max_for_limit = current_max_y
            
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.boxplot(
        plot_data,
        labels=brokers,
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


def draw_cpu_usage_graph(brokers, base_directory_name, resource_usage_data):
    for broker_name in brokers:
        fig, ax = plt.subplots(figsize=(15, 6))

        cpu_df = resource_usage_data[broker_name]['cpu']

        ax.stackplot(cpu_df.index, 
                     cpu_df['Busy User'], 
                     cpu_df['Busy System'], 
                     cpu_df['Busy Iowait'], 
                     cpu_df['Busy Other'],
                    #  cpu_df['Idle'], 
                     labels=cpu_df.columns, 
                     alpha=0.8)

        ax.set_title(f'CPU Usage (%) (Stacked) - {broker_name}')
        ax.set_ylabel('Percentage (%)')
        ax.set_xlabel('Time (minute)')

        ax.set_ylim(0, 100)
        ax.legend(loc='upper right')
        ax.grid(True, axis='y', linestyle='--')

        fig.tight_layout()

        save_plot(fig, base_directory_name, f'{broker_name}_cpu_usage')


def draw_memory_usage_graph(brokers, base_directory_name, resource_usage_data):
    for broker_name in brokers:
        mem_df = resource_usage_data[broker_name]['memory']

        fig, ax = plt.subplots(figsize=(15, 6))
        
        ax.stackplot(mem_df.index, 
                     mem_df['Used'], 
                     mem_df['Cache + Buffer'], 
                     mem_df['Free'], 
                     labels=['Used', 'Cache + Buffer', 'Free'], 
                     alpha=0.8)
        
        ax.set_title(f'Memory Usage (%) (Stacked) - {broker_name}')
        ax.set_ylabel('Percentage (%)')
        ax.set_xlabel('Time (minute)')
        
        ax.set_ylim(0, 100)
        ax.legend(loc='upper right')
        ax.grid(True, axis='y', linestyle='--')
        
        fig.tight_layout()
        
        save_plot(fig, base_directory_name, f'{broker_name}_memory_usage')

def draw_disk_iops_graph(brokers, base_directory_name, resource_usage_data):
    """Write IOps만 그리는 함수로 변경"""
    for broker_name in brokers:
        disk_df = resource_usage_data[broker_name]['iops']
        
        # Write IOps 그래프 (하나의 Figure)
        fig_write, ax_write = plt.subplots(figsize=(15, 6))
        ax_write.plot(disk_df.index, disk_df['Write IOps'], 
                      label='Write IOps', color='red', linestyle='-', marker='')
        
        ax_write.set_title(f'Disk IOps - Write - {broker_name}')
        ax_write.set_ylabel('IOps (Operations per second)')
        ax_write.set_xlabel('Time (minute)')
        
        ax_write.set_ylim(bottom=0) 
        ax_write.legend(loc='upper right')
        ax_write.grid(True, axis='both', linestyle='--')
        fig_write.tight_layout()
        save_plot(fig_write, base_directory_name, f'{broker_name}_disk_iops_write')
        plt.close(fig_write) 


def draw_iowait_graph(brokers, base_directory_name, resource_usage_data):
    for broker_name in brokers:
        iowait_df = resource_usage_data[broker_name]['iowait']

        fig, ax = plt.subplots(figsize=(15, 6))
        
        ax.plot(iowait_df.index, iowait_df['I/O Wait (%)'], 
                 label='I/O Wait (%)', color='orange', linestyle='-', marker='')
        
        ax.set_title(f'Disk I/O Wait Time - {broker_name}')
        ax.set_ylabel('I/O Wait (%)')
        ax.set_xlabel('Time (minute)')
        
        ax.set_ylim(bottom=0, top=0.2 if iowait_df['I/O Wait (%)'].max() < 100 else None)
        
        ax.legend(loc='upper right')
        ax.grid(True, axis='both', linestyle='--')
        
        fig.tight_layout()
        
        save_plot(fig, base_directory_name, f'{broker_name}_disk_iowait')


def draw_disk_throughput_graph(brokers, base_directory_name, resource_usage_data):
    """Write Throughput만 그리는 함수로 변경"""
    MB_PER_BYTE = 1024 * 1024
    
    for broker_name in brokers:
        throughput_df = resource_usage_data[broker_name]['throughput']
        
        # Write Throughput 그래프 (하나의 Figure)
        fig_write, ax_write = plt.subplots(figsize=(15, 6))
        ax_write.plot(throughput_df.index, throughput_df['Write Throughput (B/s)'] / MB_PER_BYTE, 
                      label='Write Throughput (MiB/s)', color='red', linestyle='-', marker='')
        
        ax_write.set_title(f'Disk Throughput - Write - {broker_name}')
        ax_write.set_ylabel('Throughput (MiB/s)') 
        ax_write.set_xlabel('Time (minute)')
        
        ax_write.set_ylim(bottom=0) 
        ax_write.legend(loc='upper right')
        ax_write.grid(True, axis='both', linestyle='--')
        fig_write.tight_layout()
        save_plot(fig_write, base_directory_name, f'{broker_name}_disk_throughput_write')
        plt.close(fig_write)