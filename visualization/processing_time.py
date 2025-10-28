import os
import pandas as pd
import matplotlib.pyplot as plt
from file_io import *

VISUALIZATION_BROKERS = "kafka,redpanda,memphis,rabbitmq".split(',')

data = {}
for broker in VISUALIZATION_BROKERS:
    start_time, end_time = read_benchmark_time(broker)
    try:
        data[broker] = read_consumer_data(broker, start_time, end_time)
    except Exception as e:
        print(e)

fig, ax = plt.subplots(figsize=(10, 6))
ax.boxplot(
    [data[d]['processing_time'] for d in data],
    labels=VISUALIZATION_BROKERS,
    patch_artist=True,
    boxprops=dict(facecolor='skyblue', color='black'),
    medianprops=dict(color='black', linewidth=1),
    whiskerprops=dict(color='black', linewidth=1.5),
    capprops=dict(color='black', linewidth=1.5),
    showfliers=False
)

ax.set_yscale('log')

ax.set_title("Processing Time Distribution Across Brokers", fontsize=14)
ax.set_ylabel("Processing Time (μs, log10 scale)", fontsize=12)
ax.set_xlabel("Brokers", fontsize=12)

output_file = "processing_time_boxplot.png"
fig.savefig(output_file, dpi=300, bbox_inches='tight')
plt.close(fig)

print(f"Box plot saved as {output_file}")
