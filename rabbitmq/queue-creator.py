import os
import time
import pika
from pika.exceptions import AMQPConnectionError, ConnectionWrongStateError


RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_BROKER_PORT = int(os.getenv('RABBITMQ_BROKER_PORT', 5672))
RABBITMQ_QUEUE_PREFIX = os.getenv('RABBITMQ_QUEUE_PREFIX')
PARTITION_COUNT = int(os.getenv('PARTITION_COUNT', 3)) 

RABBITMQ_EXCHANGE_NAME = f'{RABBITMQ_QUEUE_PREFIX}-exchange' 

MAX_RETRIES = 30
WAIT_TIME = 2


def create_queues():
    for attempt in range(MAX_RETRIES):
        connection = None
        try:
            connection, channel = initialize(attempt)

            channel.exchange_declare(exchange=RABBITMQ_EXCHANGE_NAME, exchange_type='direct', durable=True)
            
            queue_names = []
            for i in range(PARTITION_COUNT):
                queue_name = f'{RABBITMQ_QUEUE_PREFIX}-{i}'
                channel.queue_declare(queue=queue_name, durable=True)
                channel.queue_bind(
                    exchange=RABBITMQ_EXCHANGE_NAME, 
                    queue=queue_name, 
                    routing_key=queue_name
                )
                queue_names.append(queue_name)
            
            print(f"Successfully created and bound {PARTITION_COUNT} queues: {', '.join(queue_names)}")
            
            channel.close()
            close_connection(connection)
            return            
        except AMQPConnectionError as e:
            print(f"Connection failed: {e}. Retrying in {WAIT_TIME} seconds...")
            time.sleep(WAIT_TIME)
        except ConnectionWrongStateError as e:
            return
        except Exception as e:
            raise e
        finally:
            close_connection(connection)

    raise Exception(f"Failed to connect and create queues after {MAX_RETRIES} attempts.")


def initialize(attempt):
    print(f"Attempting to connect to RabbitMQ Broker: {RABBITMQ_HOST}:{RABBITMQ_BROKER_PORT} (Attempt {attempt + 1}/{MAX_RETRIES})")
            
    connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_BROKER_PORT
            ))
            
    channel = connection.channel()
    return connection,channel


def close_connection(connection):
    if connection and connection.is_open:
        try:
            connection.close()
        except ConnectionWrongStateError:
            pass
        except Exception as e:
            raise e


if __name__ == "__main__":
    create_queues()
