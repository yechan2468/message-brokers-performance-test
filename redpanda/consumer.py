import time
import json
import csv
from confluent_kafka import Consumer, KafkaException

TOPIC = 'redpanda'
BROKER = 'localhost:19092'
GROUP_ID = 'consumer-group'

STAT_INTERVAL_MS = 10 * 1000

RESULT_CSV_FILENAME = 'consumer_metrics.csv'

consumer_stats = {"rx_bytes": [0], "consumer_lag": [{}]}


def stats_callback(stats_json_str):
    global consumer_stats
    stats = json.loads(stats_json_str)
    consumer_stats["rx_bytes"].append(stats["rx_bytes"])
    
    partitions = stats.get("topics", {}).get(TOPIC, {}).get("partitions", {})
    lag_info = {int(partition): info.get("consumer_lag", 0) for partition, info in partitions.items()}
    consumer_stats["consumer_lag"].append(lag_info)


def initialize():
    consumer = Consumer({
        'bootstrap.servers': BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'stats_cb': stats_callback,
        'statistics.interval.ms': STAT_INTERVAL_MS,
        # 'security.protocol': "SASL_PLAINTEXT",
        # 'sasl.mechanism': "SCRAM-SHA-256",
        # 'sasl.username': "username",
        # 'sasl.password': "password",
    })
    consumer.subscribe([TOPIC])
    return consumer


def benchmark(consumer, results):
    while True:
        message = consumer.poll(timeout=1.0)
            
        if message is None:
            continue
        if message.error():
            # if message.error().code() == KafkaException._PARTITION_EOF:
            #     continue
            print(f"Consumer error: {message.error()}")
            break
            
        receive_time = time.time()
        payload_size = len(message)
        latency = receive_time - message.timestamp()[1] / 1000.0  # in seconds
            
        rx_bytes = consumer_stats["rx_bytes"][-1]
        consumer_lag = consumer_stats["consumer_lag"][-1]

        results.append([receive_time, payload_size, rx_bytes, latency, consumer_lag])
    
    return results


def write_results_to_csv(results):
    with open(RESULT_CSV_FILENAME, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['timestamp', 'message_size', 'byte_size', 'latency', 'lag'])
        csv_writer.writerows(results)


def main():
    global consumer_stats
    consumer = initialize()
    
    results = []
    try:
        benchmark(consumer, results)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print('writing results to csv...')
        write_results_to_csv(results)
        print('done.')   
        

if __name__ == "__main__":
    main()
