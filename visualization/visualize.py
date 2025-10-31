import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from file_io import save_plot


load_dotenv()

VISUALIZATION_BROKERS = os.getenv('VISUALIZATION_BROKERS').split(',') # deprecated. don't use
VISUALIZATION_RESULTS_DIR = os.getenv('VISUALIZATION_RESULTS_DIR')

BROKER_COLORS = {
    'kafka': 'red',
    'memphis': 'purple',
    'rabbitmq': 'blue',
    'redpanda': 'orange'
}

LINE_STYLES = {
    '1-1-1-AT_LEAST_ONCE': '-',
    '1-1-1-AT_MOST_ONCE': '--',
    '1-1-1-EXACTLY_ONCE': ':',
    '1-3-3-AT_LEAST_ONCE': '--',
    '5-3-3-AT_LEAST_ONCE': ':',
    '2-2-2-AT_LEAST_ONCE': '--',
    '4-4-4-AT_LEAST_ONCE': ':',
    '8-8-8-AT_LEAST_ONCE': '-.'
}

COMPARISON_GROUPS = {
    'delivery_mode': {
        'directories': ['1-1-1-AT_LEAST_ONCE', '1-1-1-AT_MOST_ONCE', '1-1-1-EXACTLY_ONCE'],
        'labels': {
            '1-1-1-AT_LEAST_ONCE': 'At Least Once',
            '1-1-1-AT_MOST_ONCE': 'At Most Once',
            '1-1-1-EXACTLY_ONCE': 'Exactly Once'
        },
        'filename_suffix': 'delivery_mode_comparison'
    },
    'producer_consumer_count': {
        'directories': ['1-1-1-AT_LEAST_ONCE', '1-3-3-AT_LEAST_ONCE', '5-3-3-AT_LEAST_ONCE'],
        'labels': {
            '1-1-1-AT_LEAST_ONCE': '#prod=1, #cons=1',
            '1-3-3-AT_LEAST_ONCE': '#prod=1, #cons=3',
            '5-3-3-AT_LEAST_ONCE': '#prod=5, #cons=3'
        },
        'filename_suffix': 'prod_cons_comparison'
    },
    'partition_count': {
        'directories': ['1-1-1-AT_LEAST_ONCE', '2-2-2-AT_LEAST_ONCE', '4-4-4-AT_LEAST_ONCE', '8-8-8-AT_LEAST_ONCE'],
        'labels': {
            '1-1-1-AT_LEAST_ONCE': '#partition=1',
            '2-2-2-AT_LEAST_ONCE': '#partition=2',
            '4-4-4-AT_LEAST_ONCE': '#partition=4',
            '8-8-8-AT_LEAST_ONCE': '#partition=8'
        },
        'filename_suffix': 'partition_comparison'
    }
}


def _get_metric_data(df, metric_type):
    """지표 유형에 따라 그래프에 사용할 데이터를 반환 (Read 데이터 제거)"""
    if df.empty:
        if metric_type == 'cpu':
            return pd.Series(dtype=float), 'Total CPU Usage (%)'
        elif metric_type == 'memory':
            return pd.Series(dtype=float), 'Total Used Memory (%)'
        elif metric_type == 'iowait':
            return pd.Series(dtype=float), 'I/O Wait (%)'
        elif metric_type == 'iops':
            return pd.Series(dtype=float), 'Write IOps (Operations per second)'
        elif metric_type == 'throughput':
            return pd.Series(dtype=float), 'Write Throughput (MiB/s)'
        return pd.Series(), ''

    if metric_type == 'cpu':
        # CPU 사용량: Busy User, System, Iowait, Other의 합산
        return df['Busy User'] + df['Busy System'] + df['Busy Iowait'] + df['Busy Other'], 'Total CPU Usage (%)'
    elif metric_type == 'memory':
        # 메모리 사용량: Used와 Cache + Buffer의 합산
        return df['Used'] + df['Cache + Buffer'], 'Total Used Memory (%)'
    elif metric_type == 'iowait':
        return df['I/O Wait (%)'], 'I/O Wait (%)'
    elif metric_type == 'iops':
        # Write IOps만 반환
        return df['Write IOps'], 'Write IOps (Operations per second)'
    elif metric_type == 'throughput':
        # Disk Write Throughput만 반환
        MB_PER_BYTE = 1024 * 1024
        return df['Write Throughput (B/s)'] / MB_PER_BYTE, 'Write Throughput (MiB/s)'
    return pd.Series(), '' # 에러 방지


def _plot_resource_comparison(
    brokers, 
    group_name,
    metric_type, 
    comparison_group_info, 
    all_resource_data
):
    """
    단일 지표에 대해 지정된 디렉토리들을 비교하는 그래프를 생성하고 저장 (듀얼 메트릭 로직 제거)
    """
    directories = comparison_group_info['directories']
    labels = comparison_group_info['labels']
    filename_suffix = comparison_group_info['filename_suffix']
    
    # iops와 throughput도 이제 단일 지표로 처리됩니다.
    metric_title = metric_type.replace('_', ' ').title()
    if metric_type == 'iops':
        metric_title = 'Write IOps'
    elif metric_type == 'throughput':
        metric_title = 'Write Throughput'


    # A. 단일 브로커 개별 그래프 (1-A)
    for broker in brokers:
        fig, ax = plt.subplots(figsize=(15, 6))
        ax.set_title(f'Comparison of {metric_title} - {broker} ({group_name})', fontsize=14)
        ax.set_xlabel('Time (minute)', fontsize=12)

        
        _, y_label = _get_metric_data(pd.DataFrame(), metric_type)
        ax.set_ylabel(y_label, fontsize=12)
        
        # 비교 그룹(디렉토리)별로 라인을 그립니다.
        for directory in directories:
            if directory not in all_resource_data or broker not in all_resource_data[directory]: continue

            # 해당 디렉토리와 브로커의 데이터프레임
            df = all_resource_data[directory][broker].get(metric_type, pd.DataFrame())
            if df.empty: continue
            
            # _get_metric_data 함수를 통해 Series를 얻음
            plot_data, _ = _get_metric_data(df, metric_type)
            
            if plot_data.empty: continue
            
            # 스타일 및 라벨 정의
            linestyle = LINE_STYLES.get(directory, '-') 
            legend_label = labels.get(directory, directory) # 'At Least Once'

            color = BROKER_COLORS.get(broker, 'gray') # A 섹션에서는 브로커 색상을 고정

            # 단일 지표 처리
            ax.plot(plot_data.index, plot_data, 
                    label=legend_label,
                    color=color, 
                    linestyle=linestyle)
                
        ax.set_ylim(bottom=0)
        ax.legend(loc='upper left', fontsize=9)
        ax.grid(True, axis='both', linestyle='--')
        fig.tight_layout()
        # 저장 파일명: 그룹명_지표_브로커_individual_comparison
        save_plot(fig, directories[0], f'{group_name}_{metric_type}_{broker}_individual_comparison') 
        plt.close(fig)

    # B. 전체 브로커 한 번에 표시 (1-B)
    # 단일 지표이므로, 하나의 Figure만 생성합니다.
    
    fig, ax = plt.subplots(figsize=(18, 8))
    ax.set_title(f'Comparison of {metric_title} Across All Brokers ({group_name})', fontsize=14)
    ax.set_xlabel('Time (minute)', fontsize=12)
    
    # Y축 레이블 설정 
    _, y_label = _get_metric_data(pd.DataFrame(), metric_type)
    ax.set_ylabel(y_label, fontsize=12)


    # 브로커별, 디렉토리별로 라인을 그립니다.
    for broker in brokers:
        for directory in directories:
            if directory not in all_resource_data or broker not in all_resource_data[directory]: continue

            df = all_resource_data[directory][broker].get(metric_type, pd.DataFrame())
            if df.empty: continue
                
            plot_data_full, _ = _get_metric_data(df, metric_type)
            
            if plot_data_full.empty: continue
            
            # 스타일 및 라벨 정의
            linestyle = LINE_STYLES.get(directory, '-')
            legend_label = labels.get(directory, directory) # 'At Least Once'
            color = BROKER_COLORS.get(broker, 'gray')
            
            # 최종 범례 라벨: 'kafka (At Least Once)'
            full_legend_label = f'{broker} ({legend_label})' 
            
            # 단일 지표이므로 plot_data_full을 그대로 사용
            ax.plot(plot_data_full.index, plot_data_full, 
                    label=full_legend_label,
                    color=color, 
                    linestyle=linestyle)

    ax.set_ylim(bottom=0)
    ax.legend(loc='upper left', bbox_to_anchor=(0, 1), fontsize=9, ncol=len(brokers)) # 범례를 보기 쉽게 조정
    ax.grid(True, axis='both', linestyle='--')
    fig.tight_layout()
    
    # 저장 파일명: 그룹_지표_all_brokers_comparison
    save_plot(fig, directories[0], f'{group_name}_{metric_type}_all_brokers_comparison')
    plt.close(fig)


def draw_comparison_resource_graphs(brokers, all_resource_data):
    """
    모든 비교 그룹 및 모든 리소스 지표에 대해 비교 그래프를 생성하는 메인 함수
    """
    
    resource_metrics = ['cpu', 'memory', 'iowait', 'iops', 'throughput']
    
    for group_name, info in COMPARISON_GROUPS.items():
        print(f"Drawing comparison graphs for group: {group_name}...")
        for metric in resource_metrics:
            _plot_resource_comparison(brokers, group_name, metric, info, all_resource_data)
        print("done.")


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
        save_plot(fig, base_directory_name, f"{broker}_throughput_byterate_boxplot")


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
        save_plot(fig, base_directory_name, f"{broker}_consumer_throughput_byterate_boxplot")


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
        save_plot(fig, base_directory_name, f"{title.replace(' ', '_').lower()}")


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
        save_plot(fig, base_directory_name, f"{title.replace(' ', '_').lower()}")



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


def draw_lag_graph(base_directory_name, consumer_data):
    fig, ax = plt.subplots(figsize=(12, 6))
    for broker in consumer_data:
        ax.plot(consumer_data[broker]['timestamp'], consumer_data[broker]['lag'], label=broker)

    ax.set_title("Consumer Lag Over Time")
    ax.set_xlabel("Time")
    ax.set_ylabel("Lag")
    ax.legend()
    plt.tight_layout()
    save_plot(fig, base_directory_name, 'lag')


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